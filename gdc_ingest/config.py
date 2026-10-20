"""Load and validate pipeline settings from TOML."""

from dataclasses import dataclass
import json
from pathlib import Path
import tomllib
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class PipelineConfig:
    endpoint_url: str
    page_size: int
    fields: list[str]
    additional_filter: dict[str, Any] | None
    raw_dir: Path
    cleaned_dir: Path
    rejected_dir: Path
    max_attempts: int
    base_delay_seconds: float
    backoff_multiplier: float
    max_delay_seconds: float


def load_config(path: Path) -> PipelineConfig:
    """Read TOML settings, resolving output paths beside the config file."""
    with path.open("rb") as config_file:
        raw = tomllib.load(config_file)

    gdc = raw["gdc"]
    output = raw["output"]
    retry = raw["retry"]
    base_url = str(gdc["base_url"]).rstrip("/")
    endpoint = str(gdc["endpoint"]).strip("/")
    parsed_url = urlparse(base_url)
    if parsed_url.scheme != "https" or parsed_url.hostname != "api.gdc.cancer.gov":
        raise ValueError("GDC base_url must use https://api.gdc.cancer.gov")
    if endpoint != "files":
        raise ValueError("this CLI only supports the public GDC files endpoint")

    page_size = int(gdc["page_size"])
    max_attempts = int(retry["max_attempts"])
    base_delay = float(retry["base_delay_seconds"])
    multiplier = float(retry["backoff_multiplier"])
    max_delay = float(retry["max_delay_seconds"])
    if page_size < 1 or max_attempts < 1:
        raise ValueError("page_size and max_attempts must be positive")
    if base_delay < 0 or multiplier < 1 or max_delay < 0:
        raise ValueError("retry delays must be nonnegative and multiplier at least 1")

    additional_json = str(gdc.get("additional_filter_json", "")).strip()
    additional_filter = json.loads(additional_json) if additional_json else None
    if additional_filter is not None and not isinstance(additional_filter, dict):
        raise ValueError("additional_filter_json must decode to a JSON object")

    config_root = path.resolve().parent

    def resolve_output_path(key: str) -> Path:
        output_path = Path(output[key])
        return output_path if output_path.is_absolute() else config_root / output_path

    return PipelineConfig(
        endpoint_url=f"{base_url}/{endpoint}",
        page_size=page_size,
        fields=list(gdc["fields"]),
        additional_filter=additional_filter,
        raw_dir=resolve_output_path("raw_dir"),
        cleaned_dir=resolve_output_path("cleaned_dir"),
        rejected_dir=resolve_output_path("rejected_dir"),
        max_attempts=max_attempts,
        base_delay_seconds=base_delay,
        backoff_multiplier=multiplier,
        max_delay_seconds=max_delay,
    )
