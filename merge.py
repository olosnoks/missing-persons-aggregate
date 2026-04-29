#!/usr/bin/env python3
"""
Merge all scraper SQLite exports (as JSON arrays) from output/<source>.json
into a single output/merged.json with a canonical envelope.

Philosophy
----------
* Promote well-known fields to top-level canonical keys.
* Dump every source-specific field verbatim into source_data[<source>] — no
  information is discarded, structure is embraced.
* Deduplicate on a composite key: (full_name_normalized, date_of_birth, sex).
  When a person appears in multiple sources the canonical record is built from
  the best available value for each field, and ALL raw blobs are kept.

Canonical envelope
------------------
{
  // --- Identity & meta ---
  "id"              : str   (sha256 of dedup key, stable across runs)
  "_sources"        : [str] (all sources that contain this person)
  "_primary_source" : str   (source with the richest record)
  "case_url"        : str | null

  // --- Name ---
  "full_name"       : str
  "first_name"      : str | null
  "middle_name"     : str | null
  "last_name"       : str | null
  "nickname"        : str | null
  "aliases"         : str | null

  // --- Demographics ---
  "sex"             : str | null
  "race"            : str | null
  "date_of_birth"   : str | null
  "age_at_disappearance" : str | null

  // --- Physical ---
  "height"          : str | null
  "weight"          : str | null
  "eye_color"       : str | null
  "hair"            : str | null
  "build"           : str | null
  "clothing"        : str | null
  "scars_marks_tattoos" : str | null
  "medical_conditions" : str | null

  // --- Disappearance ---
  "date_missing"    : str | null
  "missing_from_city"   : str | null
  "missing_from_county" : str | null
  "missing_from_state"  : str | null
  "missing_from_text"   : str | null   // free-text fallback
  "latitude"        : float | null
  "longitude"       : float | null
  "circumstances"   : str | null
  "case_status"     : str | null
  "classification"  : str | null

  // --- Agency ---
  "agency_name"     : str | null
  "agency_phone"    : str | null
  "agency_case_number" : str | null

  // --- Cross-references ---
  "namus_id"        : str | null
  "ncic_number"     : str | null

  // --- Media ---
  "primary_image_url" : str | null
  "additional_image_urls" : [str]

  // --- Raw source blobs (unmodified, keyed by source name) ---
  "source_data" : {
    "missingsippi"  : { ... } | null,
    "namus"         : { ... } | null,
    "charleyproject": { ... } | null,
    "missinginms"   : { ... } | null,
  }

  // --- Ingest meta ---
  "merged_at" : str (ISO 8601)
}
"""

import hashlib
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _str(v: Any) -> str | None:
    """Return stripped string or None."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _first(*values) -> str | None:
    """Return the first non-None/non-empty value."""
    for v in values:
        r = _str(v)
        if r:
            return r
    return None


def _parse_additional_images(raw: Any) -> list[str]:
    """Parse additional_image_urls_json which may be a JSON string or list."""
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(u) for u in raw if u]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(u) for u in parsed if u]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def _stable_id(key: tuple) -> str:
    """SHA-256 of the dedup key as a short hex id."""
    raw = "|".join(str(k) for k in key)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Source-specific normalizers
# Each returns (canonical_dict, raw_blob)
# ---------------------------------------------------------------------------

def normalize_missingsippi(rec: dict) -> dict:
    """
    Source: MissingSippi  (table: missing_person_cases)
    Key fields: profile_number, profile_url, full_name, missing_since,
                sex, race, hair, eyes, height, weight, age_at_disappearance,
                case_details, investigating_agency_name/phone, namus_id,
                raw_field_json (catch-all dict stored as JSON string)
    """
    raw_extra = {}
    if rec.get("raw_field_json"):
        try:
            raw_extra = json.loads(rec["raw_field_json"]) or {}
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "full_name":            _str(rec.get("full_name")),
        "first_name":           None,
        "middle_name":          None,
        "last_name":            None,
        "nickname":             None,
        "aliases":              None,
        "sex":                  _str(rec.get("sex")),
        "race":                 _str(rec.get("race")),
        "date_of_birth":        None,
        "age_at_disappearance": _str(rec.get("age_at_disappearance")),
        "height":               _str(rec.get("height")),
        "weight":               _str(rec.get("weight")),
        "eye_color":            _str(rec.get("eyes")),
        "hair":                 _str(rec.get("hair")),
        "build":                None,
        "clothing":             _str(rec.get("last_seen_wearing")),
        "scars_marks_tattoos":  None,
        "medical_conditions":   None,
        "date_missing":         _str(rec.get("missing_since")),
        "missing_from_city":    None,
        "missing_from_county":  None,
        "missing_from_state":   None,
        "missing_from_text":    None,
        "latitude":             None,
        "longitude":            None,
        "circumstances":        _str(rec.get("case_details")),
        "case_status":          _str(rec.get("case_status")),
        "classification":       None,
        "agency_name":          _str(rec.get("investigating_agency_name")),
        "agency_phone":         _str(rec.get("investigating_agency_phone")),
        "agency_case_number":   _str(rec.get("agency_case_number")),
        "namus_id":             _str(rec.get("namus_id")),
        "ncic_number":          None,
        "primary_image_url":    _str(rec.get("primary_image_url")),
        "additional_image_urls": _parse_additional_images(rec.get("additional_image_urls_json")),
        "case_url":             _str(rec.get("profile_url")),
        # Raw blob — keep everything including raw_html & raw_field_json
        "__raw": {**rec, **raw_extra},
    }


def normalize_namus(rec: dict) -> dict:
    """
    Source: NamUs  (table: cases)
    Key fields: namus_case_id, public_url, full_name, first/middle/last,
                date_of_birth, missing_age, sex, race_ethnicity,
                date_missing, city/county/state_missing, lat/lon,
                physical description fields, agency fields,
                listing_payload_json / detail_payload_json (full API blobs)
    """
    # NamUs stores structured payloads — preserve them verbatim
    listing_blob = None
    detail_blob = None
    if rec.get("listing_payload_json"):
        try:
            listing_blob = json.loads(rec["listing_payload_json"])
        except (json.JSONDecodeError, TypeError):
            listing_blob = rec["listing_payload_json"]
    if rec.get("detail_payload_json"):
        try:
            detail_blob = json.loads(rec["detail_payload_json"])
        except (json.JSONDecodeError, TypeError):
            detail_blob = rec["detail_payload_json"]

    smt_parts = list(filter(None, [
        _str(rec.get("scars_and_marks")),
        _str(rec.get("tattoos")),
        _str(rec.get("piercings")),
    ]))

    medical_parts = list(filter(None, [
        _str(rec.get("medical_conditions")),
        _str(rec.get("medications")),
    ]))

    return {
        "full_name":            _str(rec.get("full_name")),
        "first_name":           _str(rec.get("first_name")),
        "middle_name":          _str(rec.get("middle_name")),
        "last_name":            _str(rec.get("last_name")),
        "nickname":             _str(rec.get("nickname")),
        "aliases":              _str(rec.get("aliases")),
        "sex":                  _str(rec.get("sex")),
        "race":                 _str(rec.get("race_ethnicity")),
        "date_of_birth":        _str(rec.get("date_of_birth")),
        "age_at_disappearance": _str(rec.get("missing_age")),
        "height":               _first(rec.get("height_text"), rec.get("height_min_inches")),
        "weight":               _first(rec.get("weight_text"), rec.get("weight_min_lbs")),
        "eye_color":            _str(rec.get("eye_color")),
        "hair":                 _str(rec.get("hair_color")),
        "build":                _str(rec.get("build")),
        "clothing":             _first(
                                    rec.get("clothing_description"),
                                    rec.get("accessory_description"),
                                ),
        "scars_marks_tattoos":  "; ".join(smt_parts) or None,
        "medical_conditions":   "; ".join(medical_parts) or None,
        "date_missing":         _str(rec.get("date_missing")),
        "missing_from_city":    _str(rec.get("city_missing")),
        "missing_from_county":  _str(rec.get("county_missing")),
        "missing_from_state":   _str(rec.get("state_name")) or _str(rec.get("state_code")),
        "missing_from_text":    None,
        "latitude":             rec.get("latitude"),
        "longitude":            rec.get("longitude"),
        "circumstances":        _str(rec.get("circumstances")),
        "case_status":          _str(rec.get("case_status")),
        "classification":       None,
        "agency_name":          _first(rec.get("agency_name"), rec.get("investigating_agency")),
        "agency_phone":         _first(rec.get("investigating_agency_phone"), rec.get("contact_phone")),
        "agency_case_number":   _str(rec.get("agency_case_number")),
        "namus_id":             _str(rec.get("namus_case_id")),
        "ncic_number":          None,
        "primary_image_url":    _first(rec.get("primary_photo_url"), rec.get("thumbnail_url")),
        "additional_image_urls": [],
        "case_url":             _first(rec.get("public_url"), rec.get("source_url")),
        "__raw": {
            **rec,
            "_listing_payload": listing_blob,
            "_detail_payload": detail_blob,
        },
    }


def normalize_charleyproject(rec: dict) -> dict:
    """
    Source: The Charley Project  (table: cases)
    Key fields: case_url, name, region, missing_since, missing_from,
                classification, sex, race, date_of_birth, age,
                height_weight, clothing, vehicle, distinguishing,
                medical_conditions, details, agency, sources
    Note: height & weight are combined in height_weight (free text)
    """
    return {
        "full_name":            _str(rec.get("name")),
        "first_name":           None,
        "middle_name":          None,
        "last_name":            None,
        "nickname":             None,
        "aliases":              None,
        "sex":                  _str(rec.get("sex")),
        "race":                 _str(rec.get("race")),
        "date_of_birth":        _str(rec.get("date_of_birth")),
        "age_at_disappearance": _str(rec.get("age")),
        "height":               _str(rec.get("height_weight")),   # combined — best effort
        "weight":               None,                             # embedded in height_weight
        "eye_color":            None,                             # not in schema
        "hair":                 None,
        "build":                None,
        "clothing":             _str(rec.get("clothing")),
        "scars_marks_tattoos":  _str(rec.get("distinguishing")),
        "medical_conditions":   _str(rec.get("medical_conditions")),
        "date_missing":         _str(rec.get("missing_since")),
        "missing_from_city":    None,
        "missing_from_county":  None,
        "missing_from_state":   _str(rec.get("region")),
        "missing_from_text":    _str(rec.get("missing_from")),
        "latitude":             None,
        "longitude":            None,
        "circumstances":        _str(rec.get("details")),
        "case_status":          None,
        "classification":       _str(rec.get("classification")),
        "agency_name":          _str(rec.get("agency")),
        "agency_phone":         None,
        "agency_case_number":   None,
        "namus_id":             None,
        "ncic_number":          None,
        "primary_image_url":    _str(rec.get("primary_image_url")),
        "additional_image_urls": _parse_additional_images(rec.get("additional_image_urls_json")),
        "case_url":             _str(rec.get("case_url")),
        "__raw": {**rec},
    }


def normalize_missinginms(rec: dict) -> dict:
    """
    Source: MS Repository for Missing & Unidentified Persons  (table: missing_person_cases)
    Key fields: profile_number, profile_url, first/middle/last/full_name,
                general_age, sex, race_ancestry, eye_color, hair_description,
                clothing_description, scars, tattoos, city/county/state,
                lat/lon, agency fields, date_missing/last_seen,
                namus_entry, ncic_entered/report, case_circumstances,
                raw_field_json (catch-all)
    """
    raw_extra = {}
    if rec.get("raw_field_json"):
        try:
            raw_extra = json.loads(rec["raw_field_json"]) or {}
        except (json.JSONDecodeError, TypeError):
            pass

    smt_parts = list(filter(None, [
        _str(rec.get("scars")),
        _str(rec.get("tattoos")),
        _str(rec.get("antemortem_trauma_prior_injuries")),
    ]))

    return {
        "full_name":            _str(rec.get("full_name")),
        "first_name":           _str(rec.get("first_name")),
        "middle_name":          _str(rec.get("middle_name")),
        "last_name":            _str(rec.get("last_name")),
        "nickname":             None,
        "aliases":              None,
        "sex":                  _str(rec.get("sex")),
        "race":                 _str(rec.get("race_ancestry")),
        "date_of_birth":        None,
        "age_at_disappearance": _str(rec.get("general_age")),
        "height":               None,
        "weight":               None,
        "eye_color":            _str(rec.get("eye_color")),
        "hair":                 _str(rec.get("hair_description")),
        "build":                None,
        "clothing":             _str(rec.get("clothing_description")),
        "scars_marks_tattoos":  "; ".join(smt_parts) or None,
        "medical_conditions":   _str(rec.get("other_information_description")),
        "date_missing":         _first(
                                    rec.get("date_missing"),
                                    rec.get("date_last_seen"),
                                ),
        "missing_from_city":    _str(rec.get("city")),
        "missing_from_county":  _str(rec.get("county")),
        "missing_from_state":   _str(rec.get("state")),
        "missing_from_text":    None,
        "latitude":             rec.get("latitude"),
        "longitude":            rec.get("longitude"),
        "circumstances":        _first(
                                    rec.get("case_circumstances"),
                                    rec.get("case_notes"),
                                ),
        "case_status":          _first(
                                    rec.get("case_status"),
                                    rec.get("repository_status"),
                                ),
        "classification":       None,
        "agency_name":          _str(rec.get("agency_name")),
        "agency_phone":         _str(rec.get("agency_telephone")),
        "agency_case_number":   None,
        "namus_id":             _str(rec.get("namus_entry")),
        "ncic_number":          _first(
                                    rec.get("ncic_entered"),
                                    rec.get("ncic_report"),
                                ),
        "primary_image_url":    _str(rec.get("primary_image_url")),
        "additional_image_urls": _parse_additional_images(rec.get("additional_image_urls_json")),
        "case_url":             _str(rec.get("profile_url")),
        "__raw": {**rec, **raw_extra},
    }


# ---------------------------------------------------------------------------
# Source registry  →  maps filename stem to normalizer function
# ---------------------------------------------------------------------------

SOURCE_NORMALIZERS = {
    "missingsippi-ingest":    normalize_missingsippi,
    "namus-ingest":           normalize_namus,
    "charleyproject-ingest":  normalize_charleyproject,
    "missinginms-ingest":     normalize_missinginms,
}

# Friendly display names used as keys in source_data
SOURCE_DISPLAY = {
    "missingsippi-ingest":    "missingsippi",
    "namus-ingest":           "namus",
    "charleyproject-ingest":  "charleyproject",
    "missinginms-ingest":     "missinginms",
}


# ---------------------------------------------------------------------------
# Dedup key
# ---------------------------------------------------------------------------

def dedup_key(canon: dict) -> tuple:
    name = (canon.get("full_name") or "").lower().strip()
    dob  = (canon.get("date_of_birth") or "").strip()
    sex  = (canon.get("sex") or "").lower().strip()
    return (name, dob, sex)


# ---------------------------------------------------------------------------
# Canonical record builder — merges multiple per-source records
# ---------------------------------------------------------------------------

CANONICAL_FIELDS = [
    "full_name", "first_name", "middle_name", "last_name", "nickname", "aliases",
    "sex", "race", "date_of_birth", "age_at_disappearance",
    "height", "weight", "eye_color", "hair", "build", "clothing",
    "scars_marks_tattoos", "medical_conditions",
    "date_missing", "missing_from_city", "missing_from_county",
    "missing_from_state", "missing_from_text", "latitude", "longitude",
    "circumstances", "case_status", "classification",
    "agency_name", "agency_phone", "agency_case_number",
    "namus_id", "ncic_number",
    "primary_image_url", "additional_image_urls", "case_url",
]


def build_canonical(records: list[tuple[str, dict]]) -> dict:
    """
    records: list of (source_stem, canonical_dict)
    Returns a single merged canonical record.
    """
    # For each canonical field, pick the first non-null value across sources,
    # preferring namus > missinginms > missingsippi > charleyproject
    SOURCE_PRIORITY = [
        "namus-ingest",
        "missinginms-ingest",
        "missingsippi-ingest",
        "charleyproject-ingest",
    ]
    # Reorder records by priority
    priority_map = {s: i for i, s in enumerate(SOURCE_PRIORITY)}
    sorted_records = sorted(records, key=lambda x: priority_map.get(x[0], 99))

    merged: dict = {}
    source_data: dict = {}

    for source_stem, canon in sorted_records:
        display = SOURCE_DISPLAY.get(source_stem, source_stem)
        source_data[display] = canon.get("__raw")

        for field in CANONICAL_FIELDS:
            if merged.get(field) is None:
                val = canon.get(field)
                if val is not None and val != [] and val != "":
                    merged[field] = val

    # Ensure all canonical fields present
    for field in CANONICAL_FIELDS:
        merged.setdefault(field, None if field != "additional_image_urls" else [])

    key = dedup_key(merged)
    merged["id"] = _stable_id(key)
    merged["_sources"] = sorted(SOURCE_DISPLAY.get(s, s) for s, _ in sorted_records)
    merged["_primary_source"] = SOURCE_DISPLAY.get(sorted_records[0][0], sorted_records[0][0])
    merged["source_data"] = source_data
    merged["merged_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return merged


# ---------------------------------------------------------------------------
# Load / normalise a source file
# ---------------------------------------------------------------------------

def load_and_normalize(path: Path) -> list[tuple[str, dict]]:
    """
    Returns list of (source_stem, canonical_dict) tuples.
    """
    stem = path.stem
    normalizer = SOURCE_NORMALIZERS.get(stem)

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        log.warning(f"{stem}: file not found, skipping")
        return []
    except json.JSONDecodeError as e:
        log.error(f"{stem}: JSON parse error — {e}")
        return []

    if not isinstance(data, list):
        log.warning(f"{stem}: expected JSON array, got {type(data).__name__}, skipping")
        return []

    if normalizer is None:
        log.warning(f"{stem}: no normalizer registered, using passthrough")
        # Passthrough: treat entire record as raw blob + minimal canonical fields
        results = []
        for rec in data:
            canon = {f: None for f in CANONICAL_FIELDS}
            canon["additional_image_urls"] = []
            canon["full_name"] = _str(rec.get("full_name") or rec.get("name"))
            canon["__raw"] = rec
            results.append((stem, canon))
        return results

    results = []
    for rec in data:
        try:
            canon = normalizer(rec)
            results.append((stem, canon))
        except Exception as e:
            log.error(f"{stem}: normalizer error on record — {e}")
    log.info(f"Normalized {len(results):,} records from {stem}")
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    source_files = [
        f for f in OUTPUT_DIR.glob("*.json")
        if f.stem != "merged"
    ]

    if not source_files:
        log.warning("No source JSON files found in output/ — nothing to merge")
        sys.exit(0)

    # Load & normalize all sources
    all_records: list[tuple[str, dict]] = []
    for path in sorted(source_files):
        all_records.extend(load_and_normalize(path))

    log.info(f"Total records loaded (pre-dedup): {len(all_records):,}")

    # Bucket by dedup key
    buckets: dict[tuple, list[tuple[str, dict]]] = defaultdict(list)
    for source_stem, canon in all_records:
        key = dedup_key(canon)
        buckets[key].append((source_stem, canon))

    # Build canonical records
    merged_records: list[dict] = []
    for key, group in buckets.items():
        merged_records.append(build_canonical(group))

    log.info(f"Total records after dedup: {len(merged_records):,}")

    out_path = OUTPUT_DIR / "merged.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged_records, f, indent=2, ensure_ascii=False)

    log.info(f"Wrote {len(merged_records):,} records to {out_path}")


if __name__ == "__main__":
    main()
