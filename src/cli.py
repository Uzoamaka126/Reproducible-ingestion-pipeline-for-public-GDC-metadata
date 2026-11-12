"""Command line entry point for fetching raw GDC metadata pages."""

import argparse
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pydantic import ValidationError

from .config import PipelineConfig, load_config
from .query import build_files_request
from .schema import GdcFileRecord


class PageFetchError(RuntimeError):
    """Raised when a page cannot be fetched after all configured attempts."""


def _safe_error_message(error: Exception) -> str:
    """Extract useful, bounded diagnostics from structured HTTP error bodies."""
    if not isinstance(error, HTTPError):
        return " ".join(str(error).split())[:300]
    detail = ""
    try:
        body = error.read(16_384).decode("utf-8", errors="replace")
        payload = json.loads(body)

        def find_detail(value: Any) -> str:
            if isinstance(value, dict):
                # GDC and its gateways use several shapes, including nested
                # error objects and Elasticsearch-style root_cause entries.
                for key in ("message", "error_description", "detail", "reason", "error", "root_cause", "caused_by"):
                    candidate = value.get(key)
                    found = find_detail(candidate)
                    if found:
                        return found
                return ""
            if isinstance(value, list):
                for item in value[:3]:
                    found = find_detail(item)
                    if found:
                        return found
                return ""
            if isinstance(value, str):
                return value.strip()
            return ""

        detail = find_detail(payload)
    except (OSError, ValueError, UnicodeError):
        pass
    detail = " ".join(detail.split())[:240]
    if detail.lower() in {"internal server error", "internal error", "server error"}:
        detail = ""
    return f"HTTP {error.code}" + (f": {detail}" if detail else "")


def _log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, **fields}, sort_keys=True))


def _request_page(config: PipelineConfig, offset: int) -> tuple[bytes, dict[str, Any]]:
    payload = build_files_request(
        page_size=config.page_size,
        offset=offset,
        fields=config.fields,
        additional_filter=config.additional_filter,
    )
    request = Request(
        config.endpoint_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        body = response.read()
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("response root must be a JSON object")
    data = parsed.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("hits"), list):
        raise ValueError("response does not contain data.hits")
    pagination = data.get("pagination")
    if not isinstance(pagination, dict):
        raise ValueError("response does not contain pagination metadata")
    total = pagination.get("total")
    if not isinstance(total, int) or total < 0:
        raise ValueError("response pagination.total must be a nonnegative integer")
    return body, parsed


def _request_api_status(config: PipelineConfig) -> dict[str, Any]:
    request = Request(config.status_url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=60) as response:
        status = json.loads(response.read())
    if not isinstance(status, dict):
        raise ValueError("GDC status response must be a JSON object")
    if not status.get("tag") and not status.get("version"):
        raise ValueError("GDC status response has no API version")
    return status


def _fetch_api_status(config: PipelineConfig, logger: logging.Logger, run_id: str) -> dict[str, Any]:
    for attempt in range(1, config.max_attempts + 1):
        try:
            status = _request_api_status(config)
            api_version = status.get("tag") or status.get("version")
            _log_event(
                logger,
                "api_status_fetched",
                api_version=api_version,
                timestamp = datetime.now(timezone.utc).isoformat(),
                run_id=run_id
            )
            return status
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as error:
            safe_error = _safe_error_message(error)
            if attempt >= config.max_attempts:
                raise PageFetchError(
                    f"failed to fetch GDC API status after {attempt} attempts: {safe_error}"
                ) from error
            delay = min(
                config.base_delay_seconds * config.backoff_multiplier ** (attempt - 1),
                config.max_delay_seconds,
            )
            _log_event(
                logger,
                "api_status_retry",
                attempt=attempt,
                next_attempt=attempt + 1,
                delay_seconds=delay,
                run_id=run_id,
                timestamp = datetime.now(timezone.utc).isoformat(),
                error=safe_error,
            )
            time.sleep(delay)
    raise AssertionError("status retry loop exited unexpectedly")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as output_file:
        for chunk in iter(lambda: output_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch_with_retries(
    config: PipelineConfig,
    offset: int,
    logger: logging.Logger,
    run_id: str
) -> tuple[bytes, dict[str, Any]]:
    for attempt in range(1, config.max_attempts + 1):
        try:
            body, parsed = _request_page(config, offset)
            _log_event(logger, "page_fetched", offset=offset, attempt=attempt, timestamp = datetime.now(timezone.utc).isoformat(), run_id=run_id)
            return body, parsed
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as error:
            safe_error = _safe_error_message(error)
            if attempt >= config.max_attempts:
                _log_event(
                    logger,
                    "page_failed",
                    offset=offset,
                    attempts=attempt,
                    timestamp = datetime.now(timezone.utc).isoformat(),
                    run_id=run_id,
                    error=safe_error,
                )
                raise PageFetchError(
                    f"failed to fetch offset {offset} after {attempt} attempts: {safe_error}"
                ) from error
            delay = min(
                config.base_delay_seconds * config.backoff_multiplier ** (attempt - 1),
                config.max_delay_seconds,
            )
            _log_event(
                logger,
                "page_retry",
                offset=offset,
                attempt=attempt,
                timestamp = datetime.now(timezone.utc).isoformat(),
                run_id=run_id,
                next_attempt=attempt + 1,
                delay_seconds=delay,
                error=safe_error,
            )
            time.sleep(delay)
    raise AssertionError("retry loop exited unexpectedly")


def fetch_all_pages(
    config: PipelineConfig, logger: logging.Logger, record_limit: int | None = None
) -> int:
    """Fetch all pages and write raw, validated, and rejected records separately."""
    started_at = datetime.now(timezone.utc)
    run_id = started_at.strftime("%Y%m%dT%H%M%S.%fZ")
    api_status = _fetch_api_status(config, logger, run_id)
    run_dir = config.raw_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    cleaned_run_dir = config.cleaned_dir / run_id
    rejected_run_dir = config.rejected_dir / run_id
    cleaned_run_dir.mkdir(parents=True, exist_ok=False)
    rejected_run_dir.mkdir(parents=True, exist_ok=False)
    cleaned_path = cleaned_run_dir / "records.jsonl"
    rejected_path = rejected_run_dir / "records.jsonl"

    seen_file_ids = set()
    for existing_path in config.cleaned_dir.rglob("records.jsonl"):
        if existing_path == cleaned_path:
            continue
        with existing_path.open(encoding="utf-8") as existing_file:
            for line in existing_file:
                if line.strip():
                    seen_file_ids.add(json.loads(line)["file_id"])

    offset = 0
    page_count = 0
    total: int | None = None
    valid_count = 0
    cleaned_count = 0
    rejected_count = 0
    duplicate_count = 0
    records_fetched = 0
    raw_checksums: dict[str, str] = {}

    while (total is None or offset < total) and (
        record_limit is None or records_fetched < record_limit
    ):
        page_config = config
        if record_limit is not None:
            page_config = replace(
                config, page_size=min(config.page_size, record_limit - records_fetched)
            )
        body, parsed = _fetch_with_retries(page_config, offset, logger, run_id)
        if page_count == 0:
            cleaned_path.touch(exist_ok=False)
            rejected_path.touch(exist_ok=False)
        hits = parsed["data"]["hits"]
        pagination = parsed["data"]["pagination"]
        total = int(pagination["total"])
        page_path = run_dir / f"page_{offset:012d}.json"
        page_path.write_bytes(body)
        raw_checksums[page_path.name] = hashlib.sha256(body).hexdigest()
        page_count += 1
        page_duplicate_count = 0
        records_fetched += len(hits)

        _log_event(
            logger,
            "raw_page_saved",
            path=str(page_path),
            timestamp = datetime.now(timezone.utc).isoformat(),
            run_id=run_id,
            offset=offset,
            records=len(hits),
            total=total,
        )

        with cleaned_path.open("a", encoding="utf-8") as cleaned_file, rejected_path.open(
            "a", encoding="utf-8"
        ) as rejected_file:
            for record_index, hit in enumerate(hits):
                try:
                    if not isinstance(hit, dict):
                        raise ValueError("record must be a JSON object")
                    validated = GdcFileRecord.model_validate(hit)
                    valid_count += 1
                    file_id = validated.file_id

                    if file_id in seen_file_ids:
                        duplicate_count += 1
                        page_duplicate_count += 1
                        continue

                    cleaned_file.write(
                        json.dumps(validated.model_dump(mode="json"), sort_keys=True) + "\n"
                    )
                    seen_file_ids.add(file_id)
                    cleaned_count += 1
                except (ValidationError, ValueError) as error:
                    rejection = {
                        "offset": offset,
                        "record_index": record_index,
                        "record": hit,
                        "errors": (
                            error.errors(include_url=False, include_context=False)
                            if isinstance(error, ValidationError)
                            else [{"message": str(error)}]
                        ),
                    }
                    rejected_file.write(json.dumps(rejection, sort_keys=True) + "\n")
                    rejected_count += 1
                    _log_event(
                        logger,
                        "record_rejected",
                        offset=offset,
                        timestamp = datetime.now(timezone.utc).isoformat(),
                        run_id=run_id,
                        record_index=record_index,
                        error_count=len(rejection["errors"]),
                    )

        if page_duplicate_count:
            _log_event(
                logger,
                "duplicate_records_skipped",
                timestamp = datetime.now(timezone.utc).isoformat(),
                run_id=run_id,
                offset=offset,
                count=page_duplicate_count,
            )

        if not hits:
            if offset < total:
                raise RuntimeError(
                    f"GDC returned an empty page at offset {offset} before total {total}"
                )
            break
        offset += len(hits)

    finished_at = datetime.now(timezone.utc)
    manifest_path = config.manifest_dir / f"{run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "timestamp": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "query": build_files_request(
            page_size=config.page_size,
            offset=0,
            fields=config.fields,
            additional_filter=config.additional_filter,
        ),
        "pagination": {
            "offset_parameter": "from",
            "page_size": config.page_size,
            "pages_fetched": page_count,
        },
        "api": {
            "files_url": config.endpoint_url,
            "status_url": config.status_url,
            "version": api_status.get("tag") or api_status.get("version"),
            "version_number": api_status.get("version"),
            "commit": api_status.get("commit"),
            "status": api_status.get("status"),
        },
        "record_counts": {
            "total_from_api": total or 0,
            "fetched": records_fetched,
            "valid": valid_count,
            "written_to_cleaned": cleaned_count,
            "rejected": rejected_count,
            "duplicates_skipped": duplicate_count,
        },
        "outputs": {
            "raw_pages_directory": str(run_dir),
            "raw_pages_sha256": raw_checksums,
            "cleaned_records": str(cleaned_path),
            "cleaned_records_sha256": _sha256(cleaned_path),
            "rejected_records": str(rejected_path),
            "rejected_records_sha256": _sha256(rejected_path),
        },
    }
    with manifest_path.open("x", encoding="utf-8") as manifest_file:
        manifest_file.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    _log_event(
        logger,
        "fetch_complete",
        pages=page_count,
        records_fetched=records_fetched,
        records_written=cleaned_count,
        timestamp = datetime.now(timezone.utc).isoformat(),
        run_id=run_id,
        duplicate_count=duplicate_count,
        total_records=total or 0,
        valid_records=valid_count,
        rejected_records=rejected_count,
        raw_dir=str(run_dir),
        cleaned_path=str(cleaned_path),
        rejected_path=str(rejected_path),
        manifest_path=str(manifest_path),
    )
    return page_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch open-access GDC file metadata pages.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.toml"),
        help="TOML settings file (default: ./config.toml)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=None,
        help="Override the configured number of records per API page",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        help="Override the configured total attempts per request",
    )
    parser.add_argument(
        "--record-limit",
        type=int,
        default=None,
        help="Stop after fetching this many records in total",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Write run outputs under this directory (raw/, cleaned/, rejected/, manifests/)",
    )
    return parser


def apply_cli_overrides(
    config: PipelineConfig,
    *,
    page_size: int | None = None,
    max_attempts: int | None = None,
    output_dir: Path | None = None,
) -> PipelineConfig:
    """Return a per-run config with supplied CLI values taking precedence."""
    updates: dict[str, Any] = {}
    if page_size is not None:
        if page_size < 1:
            raise ValueError("--page-size must be positive")
        updates["page_size"] = page_size
    if max_attempts is not None:
        if max_attempts < 1:
            raise ValueError("--max-attempts must be positive")
        updates["max_attempts"] = max_attempts
    if output_dir is not None:
        output_root = output_dir.expanduser().resolve()
        updates.update(
            raw_dir=output_root / "raw",
            cleaned_dir=output_root / "cleaned",
            rejected_dir=output_root / "rejected",
            manifest_dir=output_root / "manifests",
        )
    return replace(config, **updates)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    logger = logging.getLogger("gdc_ingest")
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
        config = apply_cli_overrides(
            config,
            page_size=args.page_size,
            max_attempts=args.max_attempts,
            output_dir=args.output_dir,
        )
        if args.record_limit is not None and args.record_limit < 1:
            raise ValueError("--record-limit must be positive")
        fetch_all_pages(config, logger, record_limit=args.record_limit)
    except (OSError, KeyError, ValueError, PageFetchError, RuntimeError) as error:
        _log_event(logger, "run_failed", error=str(error), timestamp = datetime.now(timezone.utc).isoformat(),)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
