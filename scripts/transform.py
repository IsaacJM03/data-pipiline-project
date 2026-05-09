"""Transform stage for patent analytics using pandas."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from scripts.logging_config import get_logger, log_pipeline_start, log_pipeline_end, log_stats

logger = get_logger("transform")

DEFAULT_INPUT_FILE = Path("data/processed/extracted_patent_records.csv")
DEFAULT_PROCESSED_DIR = Path("data/processed")


def snake_case(column_name: str) -> str:
    """Convert a column name into snake_case."""
    cleaned = re.sub(r"[^\w]+", "_", column_name.strip())
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    return cleaned.strip("_").lower()


def clean_base_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standard cleaning to extracted records."""
    logger.info(f"[Cleaning] Starting with {len(df):,} rows")
    initial_rows = len(df)
    
    df = df.copy()
    df.columns = [snake_case(col) for col in df.columns]
    logger.debug(f"[Cleaning] Normalized {len(df.columns)} column names")
    
    duplicates_before = len(df)
    df = df.drop_duplicates()
    duplicates_removed = duplicates_before - len(df)
    logger.info(f"[Cleaning] Removed {duplicates_removed:,} duplicate rows")

    for col in ["patent_id", "inventor_id", "assignee_id"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    logger.debug("[Cleaning] Stripped ID columns")

    if "filing_date" in df.columns:
        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df["year"] = df["filing_date"].dt.year
        logger.debug("[Cleaning] Parsed filing dates and extracted years")

    before_drop = len(df)
    df = df.dropna(subset=["patent_id", "inventor_id", "assignee_id", "filing_date"])
    rows_dropped = before_drop - len(df)
    logger.info(f"[Cleaning] Dropped {rows_dropped:,} rows with missing required fields")

    string_columns = ["title", "abstract", "classification", "name", "country", "company_name"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str).str.strip()
            df.loc[df[col] == "", col] = "Unknown"
    logger.debug("[Cleaning] Standardized string columns")

    df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")
    df["year"] = df["year"].astype(int)
    
    logger.info(f"[Cleaning] Completed: {initial_rows:,} -> {len(df):,} rows")
    return df


def split_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split cleaned frame into normalized target tables."""
    logger.info("[Table Split] Starting table split...")
    
    patents = (
        df[["patent_id", "title", "abstract", "filing_date", "year", "classification"]]
        .drop_duplicates(subset=["patent_id"])
        .sort_values("patent_id")
        .reset_index(drop=True)
    )
    logger.info(f"[Table Split] patents: {len(patents):,} unique records")

    inventors = (
        df[["inventor_id", "name", "country"]]
        .drop_duplicates(subset=["inventor_id"])
        .sort_values("inventor_id")
        .reset_index(drop=True)
    )
    logger.info(f"[Table Split] inventors: {len(inventors):,} unique records")

    companies = (
        df[["assignee_id", "company_name"]]
        .rename(columns={"assignee_id": "company_id", "company_name": "name"})
        .drop_duplicates(subset=["company_id"])
        .sort_values("company_id")
        .reset_index(drop=True)
    )
    logger.info(f"[Table Split] companies: {len(companies):,} unique records")

    relationships = (
        df[["patent_id", "inventor_id", "assignee_id"]]
        .rename(columns={"assignee_id": "company_id"})
        .drop_duplicates()
        .sort_values(["patent_id", "inventor_id", "company_id"])
        .reset_index(drop=True)
    )
    logger.info(f"[Table Split] relationships: {len(relationships):,} unique records")

    return {
        "patents": patents,
        "inventors": inventors,
        "companies": companies,
        "relationships": relationships,
    }


def save_tables(tables: dict[str, pd.DataFrame], processed_dir: Path) -> dict[str, Path]:
    """Persist transformed tables as CSV files."""
    logger.info("[Table Save] Writing tables to CSV...")
    processed_dir.mkdir(parents=True, exist_ok=True)

    targets = {
        "patents": processed_dir / "clean_patents.csv",
        "inventors": processed_dir / "clean_inventors.csv",
        "companies": processed_dir / "clean_companies.csv",
        "relationships": processed_dir / "clean_relationships.csv",
    }

    for table_name, file_path in targets.items():
        tables[table_name].to_csv(file_path, index=False)
        logger.info(f"[Table Save] {table_name}: {len(tables[table_name]):,} rows -> {file_path}")

    return targets


def run_transform(
    input_file: Path = DEFAULT_INPUT_FILE,
    processed_dir: Path = DEFAULT_PROCESSED_DIR,
) -> dict[str, Path]:
    """Run transform stage and return generated file paths."""
    log_pipeline_start("TRANSFORM STAGE")
    
    input_file = Path(input_file)
    processed_dir = Path(processed_dir)

    if not input_file.exists():
        logger.error(f"Missing extracted input file: {input_file}")
        raise FileNotFoundError(f"Missing extracted input file: {input_file}")

    logger.info(f"[Input] Loading extracted data from {input_file}")
    extracted = pd.read_csv(input_file, dtype=str)
    logger.info(f"[Input] Loaded {len(extracted):,} rows, {len(extracted.columns)} columns")
    
    logger.info("[Data Cleaning] Starting data cleaning...")
    cleaned = clean_base_dataframe(extracted)
    
    logger.info("[Table Splitting] Starting table normalization...")
    tables = split_tables(cleaned)
    
    outputs = save_tables(tables, processed_dir)
    
    log_stats("TRANSFORM STAGE SUMMARY", {
        "input_rows": len(extracted),
        "cleaned_rows": len(cleaned),
        "patents": len(tables["patents"]),
        "inventors": len(tables["inventors"]),
        "companies": len(tables["companies"]),
        "relationships": len(tables["relationships"]),
    })
    log_pipeline_end("TRANSFORM STAGE", "SUCCESS")
    
    return outputs


if __name__ == "__main__":
    outputs = run_transform()
    for table, output_file in outputs.items():
        print(f"{table}: {output_file}")
