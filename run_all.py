#!/usr/bin/env python3

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.resolve()
SCRAPERS_DIR = ROOT / "scrapers"
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Repo execution plans based on each scraper's actual CLI
SCRAPER_RUNS = {
    "missingsippi-ingest": {
        "commands": [
            ["python", "-m", "scraper.cli", "seed"],
            ["python", "-m", "scraper.cli", "hydrate"],
        ],
        "db_path": "data/missingsippi.sqlite3",
        "table": "missing_person_cases",
        "env": {
            "BASE_URL": "https://www.missingsippi.org",
            "DB_PATH": None,
            "LOG_DIR": None,
            "REQUEST_TIMEOUT": "30",
            "MAX_RETRIES": "4",
            "BACKOFF_BASE": "1.5",
            "LIST_RATE_LIMIT": "2",
            "DETAIL_RATE_LIMIT": "1",
            "CONCURRENCY": "2",
            "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        },
    },
    "namus-ingest": {
        "commands": [
            ["python", "-m", "scraper.cli", "init-db"],
            ["python", "-m", "scraper.cli", "seed"],
            ["python", "-m", "scraper.cli", "hydrate"],
            ["python", "-m", "scraper.cli", "stats"],
        ],
        "db_path": "data/namus.sqlite3",
        "table": "cases",
        "env": {
            "APP_DIR": None,
            "DATA_DIR": None,
            "LOG_DIR": None,
            "EXPORT_DIR": None,
            "DB_PATH": None,
            "SCHEMA_PATH": None,
            "BASE_URL": "https://www.namus.gov",
            "SEARCH_URL": "https://www.namus.gov/api/CaseSets/NamUs/MissingPersons/Search",
            "DETAIL_URL_TEMPLATE": "https://www.namus.gov/api/CaseSets/NamUs/MissingPersons/Cases/{case_id}",
            "REQUEST_TIMEOUT": "30",
            "MAX_RETRIES": "4",
            "BACKOFF_BASE": "2.0",
            "USER_AGENT": "namus-ingest/1.0 (+personal research use)",
            "VERIFY_SSL": "true",
            "SEED_RATE_LIMIT": "1.0",
            "DETAIL_RATE_LIMIT": "1.0",
            "SEED_PAGE_SIZE": "100",
            "HYDRATE_BATCH_SIZE": "100",
            "HYDRATE_MAX_ATTEMPTS": "5",
            "LOG_LEVEL": "INFO",
        },
    },
    "charleyproject-ingest": {
        "commands": [
            ["python", "-m", "scraper.cli", "init-db"],
            ["python", "-m", "scraper.cli", "seed"],
            ["python", "-m", "scraper.cli", "hydrate"],
            ["python", "-m", "scraper.cli", "stats"],
        ],
        "db_path": "data/charley.sqlite3",
        "table": "cases",
        "env": {
            "BASE_URL": "https://charleyproject.org",
            "DB_PATH": None,
            "LOG_DIR": None,
            "REQUEST_TIMEOUT": "30",
            "MAX_RETRIES": "4",
            "BACKOFF_BASE": "2.0",
            "SEED_RATE_LIMIT": "1.0",
            "DETAIL_RATE_LIMIT": "1.0",
            "USER_AGENT": "charley-scraper/1.0 (+personal research use)",
        },
    },
    "missinginms-ingest": {
        "commands": [
            ["python", "-m", "scraper.cli", "seed"],
            ["python", "-m", "scraper.cli", "hydrate"],
        ],
        "db_path": "data/missinginms.sqlite3",
        "table": "missing_person_cases",
        "env": {
            "BASE_URL": "https://www.missinginms.msstate.edu",
            "LIST_PATH": "/missing-persons-search",
            "DB_PATH": None,
            "LOG_DIR": None,
            "REQUEST_TIMEOUT": "30",
            "MAX_RETRIES": "4",
            "BACKOFF_BASE": "1.5",
            "LIST_RATE_LIMIT": "2",
            "DETAIL_RATE_LIMIT": "1",
            "CONCURRENCY": "2",
            "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        },
    },
}


def python_executable(scraper_dir: Path) -> str:
    venv_candidates = [
        scraper_dir / ".venv" / "bin" / "python",
        scraper_dir / ".venv" / "Scripts" / "python.exe",
    ]
    for candidate in venv_candidates:
        if candidate.exists():
            return str(candidate)
    return sys.executable


def build_env(scraper_name: str, scraper_dir: Path) -> dict[str, str]:
    cfg = SCRAPER_RUNS[scraper_name]
    env = os.environ.copy()

    data_dir = scraper_dir / "data"
    logs_dir = scraper_dir / "logs"
    data_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    dynamic = {
        "APP_DIR": str(scraper_dir),
        "DATA_DIR": str(data_dir),
        "LOG_DIR": str(logs_dir),
        "EXPORT_DIR": str(data_dir),
        "DB_PATH": str(scraper_dir / cfg["db_path"]),
        "SCHEMA_PATH": str(scraper_dir / "scraper" / "schema.sql"),
        "OUTPUT_DIR": str(OUTPUT_DIR),
    }

    for key, value in cfg["env"].items():
        env[key] = dynamic.get(key, value)
    env.update(dynamic)
    return env


def run_subprocess(cmd: list[str], cwd: Path, env: dict[str, str]) -> None:
    rendered = " ".join(cmd)
    log.info("Running in %s: %s", cwd.name, rendered)
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {rendered}")


def run_scraper(scraper_name: str) -> bool:
    scraper_dir = SCRAPERS_DIR / scraper_name
    if not scraper_dir.exists():
        log.warning("Skipping %s — submodule dir not found", scraper_name)
        return False

    env = build_env(scraper_name, scraper_dir)
    py = python_executable(scraper_dir)

    try:
        for raw_cmd in SCRAPER_RUNS[scraper_name]["commands"]:
            cmd = [py if part == "python" else part for part in raw_cmd]
            run_subprocess(cmd, cwd=scraper_dir, env=env)

        collect_sqlite_to_json(
            db_path=Path(env["DB_PATH"]),
            table_name=SCRAPER_RUNS[scraper_name]["table"],
            output_path=OUTPUT_DIR / f"{scraper_name}.json",
        )
        return True
    except Exception as exc:
        log.exception("Scraper %s failed: %s", scraper_name, exc)
        return False


def collect_sqlite_to_json(db_path: Path, table_name: str, output_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")

    log.info("Collecting %s from %s", table_name, db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        data = [dict(row) for row in rows]
    finally:
        conn.close()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    log.info("Wrote %s rows to %s", len(data), output_path)


def main() -> None:
    results = {}
    for scraper_name in SCRAPER_RUNS:
        results[scraper_name] = run_scraper(scraper_name)

    successes = [name for name, ok in results.items() if ok]
    failures = [name for name, ok in results.items() if not ok]

    log.info("Scrapers finished — %s succeeded, %s failed", len(successes), len(failures))
    if failures:
        log.warning("Failed scrapers: %s", ", ".join(failures))

    log.info("Running merge.py")
    merge_result = subprocess.run([sys.executable, str(ROOT / "merge.py")], cwd=ROOT)
    if merge_result.returncode != 0:
        log.error("Merge step failed")
        sys.exit(1)

    log.info("Done. Merged output written to %s", OUTPUT_DIR / "merged.json")


if __name__ == "__main__":
    main()
