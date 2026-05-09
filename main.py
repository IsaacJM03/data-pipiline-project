"""Main pipeline runner for patent analytics."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from scripts.streaming_pipeline import run_streaming_pipeline
from scripts.load import run_load
from scripts.queries import run_queries
from scripts.report import generate_reports
from scripts.logging_config import get_logger
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)  # Suppress pandas downcasting warning

logger = get_logger("main")


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
    logger.info("\n" + "=" * 80)
    logger.info("STARTING FULL PIPELINE")
    logger.info("=" * 80)
    pipeline_start = time.time()
    
    # Check if all CSVs already exist
    csv_exists = all(csv_path.exists() for csv_path in EXPECTED_CSVS.values())
    
    if csv_exists:
        logger.info("[Stage 1/5] Using existing CSV files (skipping streaming pipeline)...")
        transformed_paths = EXPECTED_CSVS
    else:
        logger.info("[Stage 1/5] Building streaming patent tables...")
        transformed_paths = run_streaming_pipeline()
    
    logger.info(f"[Stage 1/5] Generated transformed files: {', '.join(Path(path).name for path in transformed_paths.values())}")

    logger.info("[Stage 2/5] Loading tables into SQLite...")
    db_path = run_load()
    logger.info(f"[Stage 2/5] Loaded SQLite database at {db_path}")

    logger.info("[Stage 3/5] Executing analytical SQL queries...")
    query_results = run_queries(db_path=db_path)
    logger.info(f"[Stage 3/5] Executed {len(query_results)} analytical queries")

    logger.info("[Stage 4/5] Generating reports...")
    summary = generate_reports(db_path=db_path)
    logger.info("[Stage 4/5] Report generation completed")

    # Optional: Enrich company names from g_assignee_disambiguated.tsv
    logger.info("[Stage 5/5] Enriching company names from PatentsView data...")
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
            logger.debug("[Enrichment] Extracting assignee TSV files...")
            ensure_tsv_from_own_zip(ASSIGNEE_TSV, ASSIGNEE_ZIP, ASSIGNEE_ZIP_URL)
            ensure_tsv_from_own_zip(PERSISTENT_ASSIGNEE_TSV, PERSISTENT_ASSIGNEE_ZIP, PERSISTENT_ASSIGNEE_ZIP_URL)
            logger.debug("[Enrichment] TSV files ready")
            
            # Build UUID → organization name map
            logger.debug("[Enrichment] Building UUID to name mapping...")
            name_map = build_uuid_to_name_map(PERSISTENT_ASSIGNEE_TSV, ASSIGNEE_TSV)
            
            if name_map:
                logger.info(f"[Enrichment] Found {len(name_map):,} company names to enrich")
                db_updated = update_database(name_map)
                csv_updated = update_csv(name_map)
                enriched_count = db_updated + csv_updated
                logger.info(f"[Enrichment] Enriched {enriched_count:,} company name entries")
            else:
                logger.warning("[Enrichment] No names found to enrich")
        except Exception as exc:
            logger.warning(f"[Enrichment] Skipped: {exc}")
    except ImportError:
        logger.warning("[Enrichment] Module not available, skipping company name enrichment")
    
    pipeline_duration = time.time() - pipeline_start
    logger.info(f"\nPipeline completed successfully in {pipeline_duration:.1f} seconds")
    logger.info("=" * 80)

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
