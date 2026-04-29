#!/usr/bin/env python3
"""
Orchestrator: runs every scraper submodule, then calls merge.py.

Each scraper is expected to:
  - Live in scrapers/<name>/
  - Have a main.py (or be a Docker-based scraper invoked via subprocess)
  - Write its output to output/<name>.json inside its own directory
"""

import subprocess
import sys
import os
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

SCRAPERS_DIR = Path(__file__).parent / "scrapers"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Map: submodule folder name -> entry point (relative to its dir)
SCRAPER_ENTRYPOINTS = {
    "missingsippi-ingest": "main.py",
    "namus-ingest": "main.py",
    "charleyproject-ingest": "main.py",
    "missinginms-ingest": "main.py",
}


def run_scraper(name: str, entrypoint: str) -> bool:
    scraper_dir = SCRAPERS_DIR / name
    if not scraper_dir.exists():
        log.warning(f"Skipping {name} — directory not found (submodule not initialised?)")
        return False

    entry = scraper_dir / entrypoint
    if not entry.exists():
        log.warning(f"Skipping {name} — entrypoint {entrypoint} not found")
        return False

    env = os.environ.copy()
    env["OUTPUT_DIR"] = str(OUTPUT_DIR)  # scrapers can honour this env var

    log.info(f"Running scraper: {name}")
    result = subprocess.run(
        [sys.executable, str(entry)],
        cwd=str(scraper_dir),
        env=env,
    )
    if result.returncode != 0:
        log.error(f"Scraper {name} exited with code {result.returncode}")
        return False

    log.info(f"Scraper {name} completed successfully")
    return True


def main():
    results = {}
    for name, entrypoint in SCRAPER_ENTRYPOINTS.items():
        results[name] = run_scraper(name, entrypoint)

    successes = [k for k, v in results.items() if v]
    failures = [k for k, v in results.items() if not v]

    log.info(f"Scrapers finished — {len(successes)} succeeded, {len(failures)} failed")
    if failures:
        log.warning(f"Failed: {failures}")

    # Always attempt merge with whatever data exists
    log.info("Running merge...")
    merge_result = subprocess.run([sys.executable, str(Path(__file__).parent / "merge.py")])
    if merge_result.returncode != 0:
        log.error("Merge step failed")
        sys.exit(1)

    log.info(f"Done. Merged output written to {OUTPUT_DIR / 'merged.json'}")


if __name__ == "__main__":
    main()
