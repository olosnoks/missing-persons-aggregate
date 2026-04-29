#!/usr/bin/env python3
"""
Merge all scraper output files from output/<source>.json into a single
output/merged.json, deduplicating on (name, dob, gender).

Expected per-source schema (each file is a JSON array of objects):
  {
    "name": str,
    "dob": str | null,
    "gender": str | null,
    "race": str | null,
    "missing_date": str | null,
    "missing_from": str | null,
    "age_at_missing": int | null,
    "case_url": str | null,
    "photo_url": str | null,
    "_source": str          # added automatically by this script
  }

Unknown extra fields are preserved.
"""

import json
import logging
import sys
from pathlib import Path
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"


def normalise_key(record: dict) -> tuple:
    """Build a dedup key from the record."""
    name = (record.get("name") or "").strip().lower()
    dob = (record.get("dob") or "").strip()
    gender = (record.get("gender") or "").strip().lower()
    return (name, dob, gender)


def load_source(path: Path, source_name: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            log.warning(f"{source_name}: expected JSON array, got {type(data).__name__}")
            return []
        for record in data:
            record["_source"] = source_name
        log.info(f"Loaded {len(data):,} records from {source_name}")
        return data
    except FileNotFoundError:
        log.warning(f"{source_name}: output file not found at {path}")
        return []
    except json.JSONDecodeError as e:
        log.error(f"{source_name}: JSON parse error — {e}")
        return []


def merge(all_records: list[dict]) -> list[dict]:
    """
    Deduplicate records. When duplicates exist, keep the record with the most
    non-null fields and append all source names to _sources.
    """
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for rec in all_records:
        key = normalise_key(rec)
        buckets[key].append(rec)

    merged = []
    for key, dupes in buckets.items():
        if len(dupes) == 1:
            best = dupes[0]
        else:
            # Pick the record with the most non-null values
            best = max(dupes, key=lambda r: sum(1 for v in r.values() if v is not None))

        # Collect all unique source names
        sources = sorted({r.get("_source", "unknown") for r in dupes})
        best["_sources"] = sources
        best["_source"] = sources[0]  # keep single _source as primary
        merged.append(best)

    return merged


def main():
    # Discover all per-source JSON files in output/
    source_files = [
        f for f in OUTPUT_DIR.glob("*.json")
        if f.stem != "merged"
    ]

    if not source_files:
        log.warning("No source JSON files found in output/ — nothing to merge")
        sys.exit(0)

    all_records: list[dict] = []
    for path in source_files:
        records = load_source(path, path.stem)
        all_records.extend(records)

    log.info(f"Total records before dedup: {len(all_records):,}")

    merged = merge(all_records)
    log.info(f"Total records after dedup: {len(merged):,}")

    out_path = OUTPUT_DIR / "merged.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    log.info(f"Wrote {len(merged):,} records to {out_path}")


if __name__ == "__main__":
    main()
