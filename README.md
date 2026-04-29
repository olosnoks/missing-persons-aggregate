# missing-persons-aggregate

Orchestrator that runs all missing persons ingest scrapers and merges their output into a single unified dataset.

## Architecture

```
missing-persons-aggregate/
├── scrapers/                    # Git submodules (one per source)
│   ├── missingsippi-ingest/
│   ├── namus-ingest/
│   ├── charleyproject-ingest/
│   └── missinginms-ingest/
├── output/                      # Merged output written here
│   └── merged.json
├── run_all.py                   # Runs every scraper in sequence
├── merge.py                     # Deduplicates & merges all scraper output
├── docker-compose.yml           # Spin up all scrapers in containers
└── requirements.txt
```

## Quick Start

### Clone with submodules
```bash
git clone --recurse-submodules https://github.com/olosnoks/missing-persons-aggregate.git
cd missing-persons-aggregate
```

### If already cloned without submodules
```bash
git submodule update --init --recursive
```

### Run everything
```bash
pip install -r requirements.txt
python run_all.py
```

Merged output lands in `output/merged.json`.

### Docker
```bash
docker-compose up --build
```

## Updating submodules
```bash
git submodule update --remote --merge
git add .
git commit -m "chore: update all submodules to latest"
```

## Merge logic

`merge.py` deduplicates records across sources using a composite key of `(name, dob, gender)`.  
Each record is tagged with a `_source` field indicating which scraper produced it.  
When the same person appears in multiple sources, all source tags are collected and the record with the most fields is kept as the canonical version.

## Sources

| Submodule | Source |
|---|---|
| missingsippi-ingest | MissingSippi |
| namus-ingest | NamUs |
| charleyproject-ingest | The Charley Project |
| missinginms-ingest | MS Dept of Public Safety — Missing Persons |
