"""Main pipeline runner for patent analytics."""

from __future__ import annotations

from scripts.extract import run_extraction
from scripts.transform import run_transform
from scripts.load import run_load
from scripts.queries import run_queries
from scripts.report import generate_reports


def run_pipeline() -> None:
    """Run extract, transform, load, query, and report stages."""
    print("[1/5] Extracting raw patent records...")
    extracted_path = run_extraction()
    print(f"Saved extracted records to {extracted_path}")

    print("[2/5] Transforming records into clean tables...")
    transformed_paths = run_transform()
    print(f"Generated transformed files: {', '.join(str(path) for path in transformed_paths.values())}")

    print("[3/5] Loading tables into SQLite...")
    db_path = run_load()
    print(f"Loaded SQLite database at {db_path}")

    print("[4/5] Executing analytical SQL queries...")
    query_results = run_queries(db_path=db_path)
    print(f"Executed {len(query_results)} analytical queries")

    print("[5/5] Generating reports...")
    generate_reports(db_path=db_path)
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
