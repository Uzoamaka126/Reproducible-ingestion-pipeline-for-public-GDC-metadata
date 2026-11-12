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
  `src.schema.GdcFileRecord` defines the cleaned fields; file name,
  category, type, format, size, checksum, state, update time, and linked case
  metadata are optional because GDC metadata is not uniformly populated.
- **Raw and cleaned data:** retain the complete response payload separately;
  cleaned records contain only the declared schema fields. Invalid records and
  records without explicit open-access status are written to a separate
  rejected-records JSONL file with their source offset and validation errors.
- **API version:** query `https://api.gdc.cancer.gov/status` and record the
  returned version with the run metadata. The `/status` endpoint provides API
  status and version information.

The request builder and schema are in `src/query.py` and
`src/schema.py`. Configuration defaults are in `config.toml`.

## Development setup

The project requires Python 3.12 or newer; CI currently uses Python 3.12 and
pins uv to 0.13.5. `pyproject.toml` declares direct dependencies, and the
committed `uv.lock` pins the resolved transitive dependencies and package
artifacts. Do not use `pip install -r requirements.txt` for a reproducible
environment: that file does not install from `uv.lock`.

From the project root, create or update the environment with:

```sh
uv lock --check
uv sync --locked --no-install-project
```

`uv lock --check` verifies that `uv.lock` matches `pyproject.toml`.
`uv sync --locked` installs exactly the locked dependency graph and refuses to
rewrite a stale lockfile. `--no-install-project` keeps the environment focused
on the declared dependencies; run the source tree directly with:

```sh
uv run --no-sync python -m src --help
```

Run the mocked API tests with the same synced environment:

```sh
uv run --no-sync python -m unittest discover -s tests -v
```

When changing dependencies, edit `pyproject.toml`, run `uv lock` to update
`uv.lock`, then rerun the setup commands above. Commit both files together.
GitHub Actions runs these same lock and environment commands on pushes and pull
requests.

`config.toml` also defines separate paths for raw responses, cleaned records,
rejected records, and run manifests. Retry settings specify the total attempts,
initial retry delay, exponential growth multiplier, and maximum delay. Optional
GDC filters are supplied as a JSON string in `gdc.additional_filter_json`; the
required open-access filter remains enforced by the request builder.

## Fetch raw pages

Run the CLI through the synced uv environment from the project root:

```sh
uv run --no-sync python -m src
```

Choose another settings file with `uv run --no-sync python -m src --config path/to/config.toml`.
Optional flags override `config.toml` for that invocation only:

```sh
uv run --no-sync python -m src --page-size 500 --record-limit 1000 --max-attempts 3 --output-dir ./out
```

`--page-size` controls how many records each API request asks for. The optional
`--record-limit` caps the total number of records fetched across the run; for
example, `--page-size 10 --record-limit 10` fetches at most 10 records in one
page. Without `--record-limit`, the CLI continues through all pages reported
by the API. The record limit must be a positive integer.

Omitted flags keep their configured values. `--output-dir` puts that run's
separate outputs in `raw/`, `cleaned/`, `rejected/`, and `manifests/`
subdirectories beneath the given directory.

### Output files

Each successful API page is saved as its original JSON response under a unique
timestamped directory in the configured raw path (`data/raw` by default).
Validated records are written to JSON lines under the configured cleaned path
(`data/cleaned` by default); invalid records and their validation errors go to
the configured rejected path (`data/rejected` by default).

By default, the manifest goes in the directory configured by
`run_manifest_dir` (`data/manifests` by default). If `--output-dir ./out` is
supplied, it goes under `./out/manifests`.

### Retries

Failed requests are retried up to the configured maximum number of attempts,
including the initial attempt. The delay grows exponentially between attempts
and is capped at `max_delay_seconds`. If the limit is reached, the CLI exits
with an error.

### Run manifest

The manifest records UTC start and finish timestamps, the effective query and
pagination settings, GDC API version and commit, output paths, and SHA-256
checksums for every raw page and the cleaned and rejected JSON Lines files. Its
`record_counts` fields mean:

- `total_from_api`: total number of matching files reported by the API.
- `fetched`: number of records received across all fetched pages.
- `valid`: records that passed schema validation, including duplicates.
- `written_to_cleaned`: valid records newly written to cleaned output.
- `rejected`: records that failed schema validation.
- `duplicates_skipped`: valid records whose `file_id` was already present in
  the cleaned output.
