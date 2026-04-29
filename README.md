# missing-persons-aggregate

Aggregate orchestrator for missing persons data sources.

This repo pulls together multiple ingest repositories as Git submodules, runs their scrapers, and merges their exported JSON into a single canonical dataset while preserving all source-specific fields as raw blobs.

## Goals

- Run all scraper repos from one parent repository
- Keep each scraper independent as a submodule
- Merge records into a shared canonical schema
- Preserve messy or source-specific structure instead of forcing everything into rigid columns
- Retain full per-source payloads for downstream reprocessing and auditing

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
│   └── merged.json
├── run_all.py
├── merge.py
├── docker-compose.yml
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

## Run

Install aggregate-level dependencies:

```bash
pip install -r requirements.txt
```

Run all scrapers, then merge all available output:

```bash
python run_all.py
```

Merged output is written to:

```text
output/merged.json
```

## Docker

Build and run the stack:

```bash
docker compose up --build
```

Note: service dependency order only guarantees startup order, not full readiness, so scraper containers should still be resilient to missing dependencies or delayed availability. [web:25][web:28]

## Updating submodules

Update all scraper repos to their latest tracked commits:

```bash
git submodule update --remote --merge
git add .
git commit -m "chore: update submodules"
```

Submodule definitions live in `.gitmodules`, which Git uses to store each submodule’s path and URL. [web:19][web:20]

## Merge strategy

`merge.py` now does schema-aware normalization for each known source before deduplication. [page:3]

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

- reprocess records later with improved parsers
- inspect source-specific fields
- retain provenance
- avoid data loss caused by over-normalization

## Deduplication

Records are currently bucketed using a composite key based on:

- normalized full name
- date of birth
- sex

This is intentionally pragmatic rather than perfect. Some sources are sparse, and not every source provides the same identity fields. The merge process then builds one canonical record from the grouped source entries. [page:3]

## Source priority

When multiple sources provide conflicting values, canonical field selection currently prefers richer structured sources in this order:

1. `namus-ingest`
2. `missinginms-ingest`
3. `missingsippi-ingest`
4. `charleyproject-ingest`

This priority affects which top-level canonical value is selected, but all original source values remain available inside `source_data`. [page:3]

## Source-specific notes

### NamUs

NamUs is the richest structured source in the current stack. Its listing and detail payloads are preserved in the raw source blob so the full upstream API structure remains available. [page:3]

### Charley Project

Charley Project stores some fields as free text, including combined `height_weight`, so some canonical mappings are best-effort rather than fully structured. [page:3]

### MissingSippi and MissingInMS

These sources include raw field JSON blobs and HTML snapshots, which are preserved in the merged source data for auditability and later parsing improvements. [page:3]

## Output assumptions

`merge.py` expects each source export file in `output/` to be a JSON array, typically named like:

```text
output/missingsippi-ingest.json
output/namus-ingest.json
output/charleyproject-ingest.json
output/missinginms-ingest.json
```

`run_all.py` passes `OUTPUT_DIR` so individual scrapers can write into the shared aggregate output directory.
