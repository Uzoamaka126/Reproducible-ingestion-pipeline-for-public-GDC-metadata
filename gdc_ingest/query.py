"""Build GDC `/files` requests with an unavoidable open-access restriction."""

from collections.abc import Mapping
from typing import Any


GDC_BASE_URL = "https://api.gdc.cancer.gov"
FILES_ENDPOINT = f"{GDC_BASE_URL}/files"
DEFAULT_FIELDS = [
    "file_id",
    "file_name",
    "access",
    "data_category",
    "data_type",
    "data_format",
    "file_size",
    "md5sum",
    "state",
    "updated_datetime",
    "cases.case_id",
    "cases.project.project_id",
]


def build_files_request(
    *,
    page_size: int = 1000,
    offset: int = 0,
    fields: list[str] | None = None,
    additional_filter: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a JSON POST body for one page of open-access file metadata.

    Pagination is explicit (`size` and `from`). A caller may narrow results
    using `additional_filter`; it is always ANDed with `files.access = open`.
    """
    if page_size < 1:
        raise ValueError("page_size must be positive")
    if offset < 0:
        raise ValueError("offset cannot be negative")

    clauses: list[dict[str, Any]] = [
        {
            "op": "=",
            "content": {"field": "files.access", "value": "open"},
        }
    ]
    if additional_filter is not None:
        clauses.append(dict(additional_filter))

    return {
        "filters": {"op": "and", "content": clauses},
        "fields": fields or DEFAULT_FIELDS.copy(),
        "format": "JSON",
        "size": page_size,
        "from": offset,
    }


def require_open_access(record: Mapping[str, Any]) -> None:
    """Reject records that do not explicitly identify as open access."""
    if record.get("access") != "open":
        raise ValueError("record is not explicitly marked as open access")
