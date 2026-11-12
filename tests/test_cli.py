"""Mocked end-to-end checks for GDC fetching and output behavior."""

import hashlib
import json
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError

from src.cli import PageFetchError, fetch_all_pages, main
from src.config import PipelineConfig


class FakeResponse:
    def __init__(self, body: dict[str, object]) -> None:
        self.body = json.dumps(body).encode("utf-8")

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


class FetchPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.config = PipelineConfig(
            endpoint_url="https://api.gdc.cancer.gov/files",
            status_url="https://api.gdc.cancer.gov/status",
            page_size=2,
            fields=["file_id", "access"],
            additional_filter=None,
            raw_dir=self.root / "raw",
            cleaned_dir=self.root / "cleaned",
            rejected_dir=self.root / "rejected",
            manifest_dir=self.root / "manifests",
            max_attempts=3,
            base_delay_seconds=1.0,
            backoff_multiplier=2.0,
            max_delay_seconds=10.0,
        )
        self.logger = logging.getLogger(f"test.gdc_ingest.{id(self)}")
        self.logger.propagate = False
        self.logger.addHandler(logging.NullHandler())

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def status_response() -> FakeResponse:
        return FakeResponse(
            {"tag": "8.5.0", "version": 8, "commit": "abc123", "status": "OK"}
        )

    @staticmethod
    def page_response(hits: list[object], total: int) -> FakeResponse:
        return FakeResponse(
            {
                "data": {
                    "hits": hits,
                    "pagination": {"count": len(hits), "total": total},
                }
            }
        )

    def test_fetches_every_page_using_next_offset(self) -> None:
        page_map = {
            0: self.page_response(
                [{"file_id": "file-1", "access": "open"}, {"file_id": "file-2", "access": "open"}],
                total=3,
            ),
            2: self.page_response([{"file_id": "file-3", "access": "open"}], total=3),
        }
        offsets: list[int] = []

        def mocked_urlopen(request: object, timeout: int) -> FakeResponse:
            url = request.full_url
            if url.endswith("/status"):
                return self.status_response()
            body = json.loads(request.data)
            offsets.append(body["from"])
            return page_map[body["from"]]

        with patch("src.cli.urlopen", side_effect=mocked_urlopen):
            pages = fetch_all_pages(self.config, self.logger)

        self.assertEqual(pages, 2)
        self.assertEqual(offsets, [0, 2])
        raw_run = next(self.config.raw_dir.iterdir())
        self.assertEqual(len(list(raw_run.glob("page_*.json"))), 2)
        cleaned = next(self.config.cleaned_dir.rglob("records.jsonl"))
        self.assertEqual(len(cleaned.read_text().splitlines()), 3)

    def test_retries_failed_page_with_growing_delay_and_stops_on_success(self) -> None:
        file_attempts = 0

        def mocked_urlopen(request: object, timeout: int) -> FakeResponse:
            nonlocal file_attempts
            if request.full_url.endswith("/status"):
                return self.status_response()
            file_attempts += 1
            if file_attempts < 3:
                raise URLError("temporary failure")
            return self.page_response([{"file_id": "file-1", "access": "open"}], total=1)

        with (
            patch("src.cli.urlopen", side_effect=mocked_urlopen),
            patch("src.cli.time.sleep") as sleep,
        ):
            fetch_all_pages(self.config, self.logger)

        self.assertEqual(file_attempts, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1.0, 2.0])

    def test_gives_up_after_configured_attempt_limit(self) -> None:
        file_attempts = 0

        def mocked_urlopen(request: object, timeout: int) -> FakeResponse:
            nonlocal file_attempts
            if request.full_url.endswith("/status"):
                return self.status_response()
            file_attempts += 1
            raise URLError("unavailable")

        with (
            patch("src.cli.urlopen", side_effect=mocked_urlopen),
            patch("src.cli.time.sleep"),
        ):
            with self.assertRaises(PageFetchError):
                fetch_all_pages(self.config, self.logger)

        self.assertEqual(file_attempts, self.config.max_attempts)

    def test_quarantines_invalid_records_without_stopping_run(self) -> None:
        hits = [
            {"file_id": "file-1", "access": "open"},
            {"access": "open"},
            {"file_id": "file-3", "access": "controlled"},
        ]

        def mocked_urlopen(request: object, timeout: int) -> FakeResponse:
            if request.full_url.endswith("/status"):
                return self.status_response()
            return self.page_response(hits, total=3)

        with patch("src.cli.urlopen", side_effect=mocked_urlopen):
            fetch_all_pages(self.config, self.logger)

        cleaned = next(self.config.cleaned_dir.rglob("records.jsonl"))
        rejected = next(self.config.rejected_dir.rglob("records.jsonl"))
        self.assertEqual(len(cleaned.read_text().splitlines()), 1)
        rejected_records = [json.loads(line) for line in rejected.read_text().splitlines()]
        self.assertEqual(len(rejected_records), 2)
        self.assertEqual(rejected_records[0]["record"], hits[1])
        self.assertTrue(rejected_records[0]["errors"])

    def test_second_run_does_not_write_duplicate_file_ids(self) -> None:
        def mocked_urlopen(request: object, timeout: int) -> FakeResponse:
            if request.full_url.endswith("/status"):
                return self.status_response()
            return self.page_response([{"file_id": "file-1", "access": "open"}], total=1)

        with patch("src.cli.urlopen", side_effect=mocked_urlopen):
            fetch_all_pages(self.config, self.logger)
            fetch_all_pages(self.config, self.logger)

        cleaned_files = sorted(self.config.cleaned_dir.rglob("records.jsonl"))
        self.assertEqual(len(cleaned_files), 2)
        self.assertEqual(len(cleaned_files[0].read_text().splitlines()), 1)
        self.assertEqual(cleaned_files[1].read_text(), "")

    def test_manifest_records_query_version_counts_and_output_checksums(self) -> None:
        def mocked_urlopen(request: object, timeout: int) -> FakeResponse:
            if request.full_url.endswith("/status"):
                return self.status_response()
            return self.page_response([{"file_id": "file-1", "access": "open"}], total=1)

        with patch("src.cli.urlopen", side_effect=mocked_urlopen):
            fetch_all_pages(self.config, self.logger)

        manifest_path = next(self.config.manifest_dir.glob("*.json"))
        manifest = json.loads(manifest_path.read_text())
        cleaned_path = Path(manifest["outputs"]["cleaned_records"])
        rejected_path = Path(manifest["outputs"]["rejected_records"])
        expected_cleaned_hash = hashlib.sha256(cleaned_path.read_bytes()).hexdigest()
        raw_dir = Path(manifest["outputs"]["raw_pages_directory"])
        raw_page = next(raw_dir.glob("page_*.json"))

        self.assertEqual(manifest["api"]["version"], "8.5.0")
        self.assertEqual(manifest["api"]["commit"], "abc123")
        self.assertEqual(manifest["query"]["filters"]["content"][0]["content"]["value"], "open")
        self.assertEqual(manifest["record_counts"]["fetched"], 1)
        self.assertEqual(manifest["record_counts"]["written_to_cleaned"], 1)
        self.assertEqual(manifest["outputs"]["cleaned_records_sha256"], expected_cleaned_hash)
        self.assertEqual(
            manifest["outputs"]["rejected_records_sha256"],
            hashlib.sha256(rejected_path.read_bytes()).hexdigest(),
        )
        self.assertTrue(manifest["outputs"]["raw_pages_sha256"])
        self.assertEqual(
            manifest["outputs"]["raw_pages_sha256"][raw_page.name],
            hashlib.sha256(raw_page.read_bytes()).hexdigest(),
        )

    def test_cli_flags_override_config_for_one_run(self) -> None:
        output_root = self.root / "custom-output"
        with (
            patch("src.cli.load_config", return_value=self.config),
            patch("src.cli.fetch_all_pages") as fetch,
        ):
            result = main(
                [
                    "--page-size",
                    "500",
                    "--max-attempts",
                    "2",
                    "--output-dir",
                    str(output_root),
                ]
            )

        self.assertEqual(result, 0)
        run_config = fetch.call_args.args[0]
        self.assertEqual(run_config.page_size, 500)
        self.assertEqual(run_config.max_attempts, 2)
        self.assertEqual(run_config.raw_dir, (output_root / "raw").resolve())
        self.assertEqual(run_config.cleaned_dir, (output_root / "cleaned").resolve())
        self.assertEqual(run_config.rejected_dir, (output_root / "rejected").resolve())
        self.assertEqual(run_config.manifest_dir, (output_root / "manifests").resolve())
        self.assertEqual(self.config.page_size, 2)
        self.assertEqual(self.config.max_attempts, 3)

    def test_cli_uses_config_values_when_overrides_are_omitted(self) -> None:
        with (
            patch("src.cli.load_config", return_value=self.config),
            patch("src.cli.fetch_all_pages") as fetch,
        ):
            result = main([])

        self.assertEqual(result, 0)
        run_config = fetch.call_args.args[0]
        self.assertEqual(run_config.page_size, self.config.page_size)
        self.assertEqual(run_config.max_attempts, self.config.max_attempts)
        self.assertEqual(run_config.raw_dir, self.config.raw_dir)

    def test_cli_rejects_nonpositive_overrides_without_fetching(self) -> None:
        with (
            patch("src.cli.load_config", return_value=self.config),
            patch("src.cli.fetch_all_pages") as fetch,
        ):
            result = main(["--page-size", "0"])

        self.assertEqual(result, 1)
        fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
