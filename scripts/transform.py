"""Transform stage for patent analytics using pandas."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

DEFAULT_INPUT_FILE = Path("data/processed/extracted_patent_records.csv")
DEFAULT_PROCESSED_DIR = Path("data/processed")


def snake_case(column_name: str) -> str:
    """Convert a column name into snake_case."""
    cleaned = re.sub(r"[^\w]+", "_", column_name.strip())
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    return cleaned.strip("_").lower()


def clean_base_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standard cleaning to extracted records."""
    df = df.copy()
    df.columns = [snake_case(col) for col in df.columns]
    df = df.drop_duplicates()

    for col in ["patent_id", "inventor_id", "assignee_id"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    if "filing_date" in df.columns:
        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df["year"] = df["filing_date"].dt.year

    df = df.dropna(subset=["patent_id", "inventor_id", "assignee_id", "filing_date"])

    string_columns = ["title", "abstract", "classification", "name", "country", "company_name"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str).str.strip()
            df.loc[df[col] == "", col] = "Unknown"

    df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")
    df["year"] = df["year"].astype(int)

    return df


def split_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split cleaned frame into normalized target tables."""
    patents = (
        df[["patent_id", "title", "abstract", "filing_date", "year", "classification"]]
        .drop_duplicates(subset=["patent_id"])
        .sort_values("patent_id")
        .reset_index(drop=True)
    )

    inventors = (
        df[["inventor_id", "name", "country"]]
        .drop_duplicates(subset=["inventor_id"])
        .sort_values("inventor_id")
        .reset_index(drop=True)
    )

    companies = (
        df[["assignee_id", "company_name"]]
        .rename(columns={"assignee_id": "company_id", "company_name": "name"})
        .drop_duplicates(subset=["company_id"])
        .sort_values("company_id")
        .reset_index(drop=True)
    )

    relationships = (
        df[["patent_id", "inventor_id", "assignee_id"]]
        .rename(columns={"assignee_id": "company_id"})
        .drop_duplicates()
        .sort_values(["patent_id", "inventor_id", "company_id"])
        .reset_index(drop=True)
    )

    return {
        "patents": patents,
        "inventors": inventors,
        "companies": companies,
        "relationships": relationships,
    }


def save_tables(tables: dict[str, pd.DataFrame], processed_dir: Path) -> dict[str, Path]:
    """Persist transformed tables as CSV files."""
    processed_dir.mkdir(parents=True, exist_ok=True)

    targets = {
        "patents": processed_dir / "clean_patents.csv",
        "inventors": processed_dir / "clean_inventors.csv",
        "companies": processed_dir / "clean_companies.csv",
        "relationships": processed_dir / "clean_relationships.csv",
    }

    for table_name, file_path in targets.items():
        tables[table_name].to_csv(file_path, index=False)

    return targets


def run_transform(
    input_file: Path = DEFAULT_INPUT_FILE,
    processed_dir: Path = DEFAULT_PROCESSED_DIR,
) -> dict[str, Path]:
    """Run transform stage and return generated file paths."""
    input_file = Path(input_file)
    processed_dir = Path(processed_dir)

    if not input_file.exists():
        raise FileNotFoundError(f"Missing extracted input file: {input_file}")

    extracted = pd.read_csv(input_file, dtype=str)
    cleaned = clean_base_dataframe(extracted)
    tables = split_tables(cleaned)
    return save_tables(tables, processed_dir)


if __name__ == "__main__":
    outputs = run_transform()
    for table, output_file in outputs.items():
        print(f"{table}: {output_file}")
