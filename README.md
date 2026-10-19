# Reproducible ingestion pipeline for public GDC metadata

## GDC source contract

- **Endpoint:** `POST https://api.gdc.cancer.gov/files` (the GDC files search
  endpoint).
- **Request format:** JSON body with `format: "JSON"`, a list of requested
  `fields`, and explicit `size` / `from` pagination values. The API response
  envelope is expected to provide `data.hits` and pagination metadata.
- **Query restriction:** every request includes `files.access = "open"`. Any
  project, data type, or other filter narrows this required clause with `and`.
  Returned records are checked again locally and only records whose `access`
  value is exactly `open` can pass the cleaned record schema.
- **Record identity and schema:** `file_id` is the stable record identifier.
  `gdc_ingest.schema.GdcFileRecord` defines the cleaned fields; file name,
  category, type, format, size, checksum, state, update time, and linked case
  metadata are optional because GDC metadata is not uniformly populated.
- **Raw and cleaned data:** retain the complete response payload separately;
  cleaned records contain only the declared schema fields. Invalid records and
  records without explicit open-access status should be routed to quarantine
  when the ingestion writer is implemented.
- **API version:** query `https://api.gdc.cancer.gov/status` and record the
  returned version with the run metadata. The `/status` endpoint provides API
  status and version information.

The request builder and schema are in `gdc_ingest/query.py` and
`gdc_ingest/schema.py`. Configuration defaults are in `config.toml`.

`config.toml` also defines separate paths for raw responses, cleaned records,
rejected records, and run manifests. Retry settings specify the total attempts,
initial retry delay, exponential growth multiplier, and maximum delay. Optional
GDC filters are supplied as a JSON string in `gdc.additional_filter_json`; the
required open-access filter remains enforced by the request builder.
