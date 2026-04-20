# Patent Analytics Data Pipeline

This project provides a complete, reproducible Python data pipeline for patent analytics using USPTO PatentsView-style data.

Official granted patent disambiguated source:
- https://data.uspto.gov/bulkdata/datasets/pvgpatdis?fileDataFromDate=1976-01-01&fileDataToDate=2025-09-30
- Use the accompanying `PV_grant_data_dictionary.pdf` on that page to map raw columns when preparing inputs.

## Architecture

The pipeline follows an ETL + analytics flow:

1. **Extract** (`scripts/extract.py`)
   - Reads raw patent CSV/TSV files from `data/raw/`
   - If `data/raw/` is empty, attempts to pull source files directly from the official PatentsView granted dataset page and unpacks downloaded ZIP archives
   - Keeps relevant fields only:
     - `patent_id`, `title`, `abstract`, `filing_date`
     - `classification`
     - `inventor_id`, `name`, `country`
     - `assignee_id`, `company_name`
   - Writes `data/processed/extracted_patent_records.csv`
   - If no raw files are present, generates a deterministic sample file so the pipeline remains reproducible.

2. **Transform** (`scripts/transform.py`)
   - Cleans missing values
   - Normalizes column names to snake_case
   - Extracts `year` from `filing_date`
   - Removes duplicates
   - Produces normalized CSV tables:
     - `clean_patents.csv`
     - `clean_inventors.csv`
     - `clean_companies.csv`
     - `clean_relationships.csv`

3. **Load** (`scripts/load.py`)
   - Creates SQLite schema from `sql/schema.sql`
   - Loads cleaned tables into `data/patent_analytics.db` (including patent classification in `patents`)

4. **Query** (`scripts/queries.py` + `sql/queries.sql`)
   - Runs analytical SQL queries (top inventors/companies/countries, trend, joins, CTE, ranking)
   - Exports query outputs to `outputs/exports/`

5. **Report** (`scripts/report.py`)
   - Console summary
   - CSV exports:
     - `top_inventors.csv`
     - `top_companies.csv`
     - `country_trends.csv`
   - JSON report at `outputs/reports/summary_report.json`
   - Bonus: trend chart at `outputs/reports/patents_per_year.png`

## Project Structure

```text
project/
├── data/
│   ├── raw/
│   └── processed/
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── queries.py
│   └── report.py
├── sql/
│   ├── schema.sql
│   └── queries.sql
├── outputs/
│   ├── reports/
│   └── exports/
├── main.py
├── dashboard.py
├── requirements.txt
└── README.md
```

## Setup

1. Create and activate a Python 3 virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the Pipeline (One Command)

```bash
python main.py
```

Optional extract controls:
- `PATENTSVIEW_AUTO_DOWNLOAD=1` (default): enable pull from the official PatentsView dataset page when `data/raw/` is empty
- `PATENTSVIEW_AUTO_DOWNLOAD=0`: disable network pull and use deterministic sample fallback when no local raw files exist
- `PATENTSVIEW_MAX_DOWNLOAD_FILES=8` (default): cap number of source files downloaded from the dataset page

After completion:
- SQLite DB: `data/patent_analytics.db`
- Reports: `outputs/reports/`
- CSV exports: `outputs/exports/`

## Optional Dashboard (Bonus)

```bash
streamlit run dashboard.py
```

This loads generated report outputs and displays KPI cards and result tables.
