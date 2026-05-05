"""Main pipeline runner for patent analytics."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from scripts.streaming_pipeline import run_streaming_pipeline
from scripts.load import run_load
from scripts.queries import run_queries
from scripts.report import generate_reports
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)  # Suppress pandas downcasting warning


PROCESSED_DIR = Path("data/processed")
EXPECTED_CSVS = {
    "patents": PROCESSED_DIR / "clean_patents.csv",
    "inventors": PROCESSED_DIR / "clean_inventors.csv",
    "companies": PROCESSED_DIR / "clean_companies.csv",
    "relationships": PROCESSED_DIR / "clean_relationships.csv",
}


def print_pipeline_summary(summary: dict) -> None:
    """Print a concise terminal summary for the generated analytics outputs."""
    print("\nTerminal Summary")
    print("=" * 16)
    print(f"Total patents: {summary['total_patents']}")

    # Show processed CSV counts and a small sample for debugging
    print('\nProcessed CSVs:')
    for name, path in EXPECTED_CSVS.items():
        try:
            if path.exists():
                # Count rows without loading entire file
                with open(path, "rb") as f:
                    row_count = sum(1 for _ in f) - 1
                    if row_count < 0:
                        row_count = 0
                print(f"- {name}: {row_count:,} rows ({path})")
                # Print a small sample for quick inspection
                try:
                    import pandas as pd

                    sample = pd.read_csv(path, sep=",")
                    print(sample.head(5).to_string(index=False))
                except Exception:
                    # If CSV parsing fails, skip sample printing
                    pass
            else:
                print(f"- {name}: MISSING ({path})")
        except Exception as exc:
            print(f"- {name}: error reading file: {exc}")

    top_inventors = summary.get("top_inventors", [])
    if top_inventors:
        print("Top inventors:")
        for row in top_inventors:
            print(f"- {row['name']}: {row['patent_count']} patents")

    top_companies = summary.get("top_companies", [])
    if top_companies:
        print("Top companies:")
        for row in top_companies:
            print(f"- {row['name']}: {row['patent_count']} patents")

    top_countries = summary.get("top_countries", [])
    if top_countries:
        print("Top countries:")
        for row in top_countries:
            print(f"- {row['country']}: {row['patent_count']} patents")


def start_dashboard() -> None:
    """Launch the Streamlit dashboard after the pipeline finishes."""
    subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard.py"], check=True)


def run_pipeline() -> dict:
    """Run extract, transform, load, query, and report stages."""
    
    # Check if all CSVs already exist
    csv_exists = all(csv_path.exists() for csv_path in EXPECTED_CSVS.values())
    
    if csv_exists:
        print("[1/4] Using existing CSV files (skipping streaming pipeline)...")
        transformed_paths = EXPECTED_CSVS
    else:
        print("[1/4] Building streaming patent tables...")
        transformed_paths = run_streaming_pipeline()
    
    print(f"Generated transformed files: {', '.join(str(path) for path in transformed_paths.values())}")

    print("[2/4] Loading tables into SQLite...")
    db_path = run_load()
    print(f"Loaded SQLite database at {db_path}")
    # db_path = "data/processed/patent_staging.db"  # Assuming the database is already created by the streaming pipeline

    print("[3/4] Executing analytical SQL queries...")
    query_results = run_queries(db_path=db_path)
    print(f"Executed {len(query_results)} analytical queries")

    print("[4/4] Generating reports...")
    summary = generate_reports(db_path=db_path)
    print("Pipeline completed successfully.")

    # Optional: Enrich company names from g_assignee_disambiguated.tsv
    print("\n[Bonus] Enriching company names from PatentsView data...")
    try:
        from scripts.enrich_company_names import (
            build_uuid_to_name_map,
            update_database,
            update_csv,
            ASSIGNEE_TSV,
            PERSISTENT_ASSIGNEE_TSV,
            DB_PATH,
            CLEAN_COMPANIES_CSV,
            ensure_tsv_from_own_zip,
            ASSIGNEE_ZIP,
            ASSIGNEE_ZIP_URL,
            PERSISTENT_ASSIGNEE_ZIP,
            PERSISTENT_ASSIGNEE_ZIP_URL,
        )

        # Ensure TSV files exist
        try:
            ensure_tsv_from_own_zip(ASSIGNEE_TSV, ASSIGNEE_ZIP, ASSIGNEE_ZIP_URL)
            ensure_tsv_from_own_zip(PERSISTENT_ASSIGNEE_TSV, PERSISTENT_ASSIGNEE_ZIP, PERSISTENT_ASSIGNEE_ZIP_URL)
            
            # Build UUID → organization name map
            name_map = build_uuid_to_name_map(PERSISTENT_ASSIGNEE_TSV, ASSIGNEE_TSV)
            
            if name_map:
                db_updated = update_database(name_map)
                csv_updated = update_csv(name_map)
                print(f"  Enriched {db_updated + csv_updated:,} company name entries.")
            else:
                print("  Warning: No names to enrich.")
        except Exception as exc:
            print(f"  Skipped enrichment: {exc}")
    except ImportError:
        print("  Skipped: enrichment module not available.")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the patent analytics pipeline.")
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Launch the Streamlit dashboard after the pipeline and terminal summary finish.",
    )
    args = parser.parse_args()

    summary = run_pipeline()
    print_pipeline_summary(summary)

    if args.serve:
        start_dashboard()
