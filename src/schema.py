"""Validated shape of an open-access GDC file metadata record."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class GdcFileRecord(BaseModel):
    """Cleaned fields retained for each public GDC file record.

    `file_id` is the stable GDC UUID used for deduplication. Optional fields
    reflect that metadata completeness varies between files. Unknown API fields
    are omitted from cleaned output while the raw response remains available.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    file_id: str = Field(min_length=1)
    file_name: str | None = None
    access: Literal["open"]
    data_category: str | None = None
    data_type: str | None = None
    data_format: str | None = None
    file_size: int | None = Field(default=None, ge=0)
    md5sum: str | None = None
    state: str | None = None
    updated_datetime: datetime | None = None
    cases: list[dict[str, object]] = Field(default_factory=list)
