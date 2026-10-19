"""Command line entry point for fetching raw GDC metadata pages."""

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import PipelineConfig, load_config
from .query import build_files_request


class PageFetchError(RuntimeError):
    """Raised when a page cannot be fetched after all configured attempts."""


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


def _fetch_with_retries(
    config: PipelineConfig,
    offset: int,
    logger: logging.Logger,
) -> tuple[bytes, dict[str, Any]]:
    for attempt in range(1, config.max_attempts + 1):
        try:
            body, parsed = _request_page(config, offset)
            _log_event(logger, "page_fetched", offset=offset, attempt=attempt)
            return body, parsed
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as error:
            if attempt >= config.max_attempts:
                _log_event(
                    logger,
                    "page_failed",
                    offset=offset,
                    attempts=attempt,
                    error=str(error),
                )
                raise PageFetchError(
                    f"failed to fetch offset {offset} after {attempt} attempts: {error}"
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
                next_attempt=attempt + 1,
                delay_seconds=delay,
                error=str(error),
            )
            time.sleep(delay)
    raise AssertionError("retry loop exited unexpectedly")


def fetch_all_pages(config: PipelineConfig, logger: logging.Logger) -> int:
    """Fetch every page, saving each successful response as an immutable file."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir = config.raw_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    offset = 0
    page_count = 0
    total: int | None = None
    while total is None or offset < total:
        body, parsed = _fetch_with_retries(config, offset, logger)
        hits = parsed["data"]["hits"]
        pagination = parsed["data"]["pagination"]
        total = int(pagination["total"])
        page_path = run_dir / f"page_{offset:012d}.json"
        page_path.write_bytes(body)
        page_count += 1
        _log_event(
            logger,
            "raw_page_saved",
            path=str(page_path),
            offset=offset,
            records=len(hits),
            total=total,
        )

        if not hits:
            if offset < total:
                raise RuntimeError(
                    f"GDC returned an empty page at offset {offset} before total {total}"
                )
            break
        offset += len(hits)

    _log_event(
        logger,
        "fetch_complete",
        pages=page_count,
        total_records=total or 0,
        raw_dir=str(run_dir),
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
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    logger = logging.getLogger("gdc_ingest")
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
        fetch_all_pages(config, logger)
    except (OSError, KeyError, ValueError, PageFetchError, RuntimeError) as error:
        _log_event(logger, "run_failed", error=str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
