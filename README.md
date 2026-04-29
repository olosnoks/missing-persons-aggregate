# missing-persons-aggregate

Aggregate orchestrator for missing persons data sources.

This repo pulls together multiple ingest repositories as Git submodules, runs their scrapers from the aggregate repo, collects their SQLite data into per-source JSON, and merges the results into a single canonical dataset while preserving all source-specific fields as raw blobs.

## Goals

- Run all scraper repos from one parent repository.
- Keep each scraper independent as a submodule.
- Execute each scraper using its own real CLI workflow.
- Collect rows directly from each scraper SQLite database.
- Merge records into a shared canonical schema.
- Preserve messy or source-specific structure instead of forcing everything into rigid columns.
- Retain full per-source payloads for downstream reprocessing and auditing.

## Repository Layout

```text
missing-persons-aggregate/
├── .gitmodules
├── scrapers/
│   ├── missingsippi-ingest/
│   ├── namus-ingest/
│   ├── charleyproject-ingest/
│   └── missinginms-ingest/
├── output/
│   ├── missingsippi-ingest.json
│   ├── namus-ingest.json
│   ├── charleyproject-ingest.json
│   ├── missinginms-ingest.json
│   └── merged.json
├── run_all.py
├── merge.py
└── requirements.txt
```

## Sources

| Submodule | Source |
|---|---|
| `missingsippi-ingest` | MissingSippi |
| `namus-ingest` | NamUs |
| `charleyproject-ingest` | The Charley Project |
| `missinginms-ingest` | Mississippi Repository for Missing and Unidentified Persons |

## Clone

Clone with submodules:

```bash
git clone --recurse-submodules https://github.com/olosnoks/missing-persons-aggregate.git
cd missing-persons-aggregate
```

If needed later:

```bash
git submodule update --init --recursive
```

Submodule definitions live in `.gitmodules`, which Git uses to store each submodule’s path and URL. [web:19][web:20]

## Python environments

Each scraper should have its own virtual environment inside its submodule directory.

Typical setup inside each scraper repo:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Virtual environments isolate dependencies between projects, which matters here because each scraper is maintained independently. [web:125][web:129]

## How aggregate execution works

`run_all.py` does four things:

1. Runs each scraper from inside its own submodule directory.
2. Uses the scraper’s real CLI entrypoint, `python -m scraper.cli`.
3. Reads each scraper’s SQLite database directly with Python’s `sqlite3` module.
4. Writes a JSON array per source into `output/`, then runs `merge.py`. [web:140][web:151]

This avoids depending on CSV exports for the merge pipeline and preserves all original database columns as raw JSON-friendly data.

## CLI workflows used

The aggregate repo follows the actual CLI structure implemented by each ingest repo.

| Repo | Aggregate command flow |
|---|---|
| `missingsippi-ingest` | `seed` → `hydrate` → collect SQLite to JSON |
| `namus-ingest` | `init-db` → `seed` → `hydrate` → `stats` → collect SQLite to JSON |
| `charleyproject-ingest` | `init-db` → `seed` → `hydrate` → `stats` → collect SQLite to JSON |
| `missinginms-ingest` | `seed` → `hydrate` → collect SQLite to JSON |

These CLIs are exposed via package-style module execution, which is why the aggregate repo uses `python -m scraper.cli` instead of a top-level `main.py`. [page:1]

## Run

Install aggregate-level dependencies if you add any later:

```bash
pip install -r requirements.txt
```

Run the aggregate workflow:

```bash
python run_all.py
```

Outputs written by the aggregate repo:

```text
output/missingsippi-ingest.json
output/namus-ingest.json
output/charleyproject-ingest.json
output/missinginms-ingest.json
output/merged.json
```

## SQLite collection

The aggregate repo reads directly from these tables:

| Repo | SQLite file | Table |
|---|---|---|
| `missingsippi-ingest` | `data/missingsippi.sqlite3` | `missing_person_cases` |
| `namus-ingest` | `data/namus.sqlite3` | `cases` |
| `charleyproject-ingest` | `data/charley.sqlite3` | `cases` |
| `missinginms-ingest` | `data/missinginms.sqlite3` | `missing_person_cases` |

Using SQLite directly keeps all source columns available for later normalization and blob preservation. [web:151]

## Merge strategy

`merge.py` does schema-aware normalization for each known source before deduplication.

Currently supported source normalizers:

- `missingsippi-ingest`
- `namus-ingest`
- `charleyproject-ingest`
- `missinginms-ingest`

Each source is mapped into a shared canonical envelope, but all original source fields are still preserved.

### Canonical record shape

Each merged record contains top-level canonical fields such as:

- `id`
- `_sources`
- `_primary_source`
- `case_url`
- `full_name`
- `first_name`
- `middle_name`
- `last_name`
- `nickname`
- `aliases`
- `sex`
- `race`
- `date_of_birth`
- `age_at_disappearance`
- `height`
- `weight`
- `eye_color`
- `hair`
- `build`
- `clothing`
- `scars_marks_tattoos`
- `medical_conditions`
- `date_missing`
- `missing_from_city`
- `missing_from_county`
- `missing_from_state`
- `missing_from_text`
- `latitude`
- `longitude`
- `circumstances`
- `case_status`
- `classification`
- `agency_name`
- `agency_phone`
- `agency_case_number`
- `namus_id`
- `ncic_number`
- `primary_image_url`
- `additional_image_urls`
- `source_data`
- `merged_at`

## Raw blob preservation

A core design rule of this project is: normalize what is useful, preserve everything else.

Each merged record includes:

```json
"source_data": {
  "missingsippi": { ... },
  "namus": { ... },
  "charleyproject": { ... },
  "missinginms": { ... }
}
```

This allows downstream consumers to:

- Reprocess records later with improved parsers.
- Inspect source-specific fields.
- Retain provenance.
- Avoid data loss caused by over-normalization.

## Deduplication

Records are currently bucketed using a composite key based on:

- Normalized full name
- Date of birth
- Sex

This is intentionally pragmatic rather than perfect. Some sources are sparse, and not every source provides the same identity fields. The merge process then builds one canonical record from the grouped source entries.

## Source priority

When multiple sources provide conflicting values, canonical field selection currently prefers richer structured sources in this order:

1. `namus-ingest`
2. `missinginms-ingest`
3. `missingsippi-ingest`
4. `charleyproject-ingest`

This priority affects which top-level canonical value is selected, but all original source values remain available inside `source_data`.

## Source-specific notes

### NamUs

NamUs is the richest structured source in the current stack. Its listing and detail payloads are preserved in the raw source blob so the full upstream API structure remains available.

### Charley Project

Charley Project stores some fields as free text, including combined `height_weight`, so some canonical mappings are best-effort rather than fully structured.

### MissingSippi and MissingInMS

These sources include raw field JSON blobs and HTML snapshots, which are preserved in the merged source data for auditability and later parsing improvements.

## Updating submodules

Update all scraper repos to their latest tracked commits:

```bash
git submodule update --remote --merge
git add .
git commit -m "chore: update submodules"
```

## Notes

- `run_all.py` uses subprocesses with per-repo working directories so each CLI runs in the environment and filesystem layout it expects. [web:140]
- SQLite access is handled through Python’s standard `sqlite3` module. [web:151]
- The aggregate repo no longer depends on per-repo CSV exports for merging.
