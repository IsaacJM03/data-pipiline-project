"""Main pipeline runner for patent analytics."""

from __future__ import annotations

from pathlib import Path

from scripts.streaming_pipeline import run_streaming_pipeline
from scripts.load import run_load
from scripts.queries import run_queries
from scripts.report import generate_reports


PROCESSED_DIR = Path("data/processed")
EXPECTED_CSVS = {
    "patents": PROCESSED_DIR / "clean_patents.csv",
    "inventors": PROCESSED_DIR / "clean_inventors.csv",
    "companies": PROCESSED_DIR / "clean_companies.csv",
    "relationships": PROCESSED_DIR / "clean_relationships.csv",
}


def run_pipeline() -> None:
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

    print("[3/4] Executing analytical SQL queries...")
    query_results = run_queries(db_path=db_path)
    print(f"Executed {len(query_results)} analytical queries")

    print("[4/4] Generating reports...")
    generate_reports(db_path=db_path)
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
