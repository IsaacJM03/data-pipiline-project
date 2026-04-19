"""Extract stage for USPTO PatentsView-like raw files."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Iterable

import pandas as pd

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_OUTPUT_FILE = Path("data/processed/extracted_patent_records.csv")

RELEVANT_FIELDS = {
    "patent_id": ["patent_id", "patent", "id"],
    "title": ["title", "patent_title"],
    "abstract": ["abstract", "patent_abstract"],
    "filing_date": ["filing_date", "date", "patent_date"],
    "inventor_id": ["inventor_id", "inventor", "inventor_key_id"],
    "name": ["name", "inventor_name", "inventor_full_name"],
    "country": ["country", "inventor_country"],
    "assignee_id": ["assignee_id", "company_id", "organization_id"],
    "company_name": ["company_name", "assignee_name", "company", "organization_name"],
}


SAMPLE_DATA = [
    {
        "patent_id": "US1000001",
        "title": "Battery cooling optimization",
        "abstract": "System and method for electric battery thermal regulation.",
        "filing_date": "2018-04-10",
        "inventor_id": "INV001",
        "name": "Alicia Gomez",
        "country": "US",
        "assignee_id": "CMP001",
        "company_name": "Voltix Labs",
    },
    {
        "patent_id": "US1000002",
        "title": "AI scheduling for manufacturing",
        "abstract": "Machine learning model to optimize factory throughput.",
        "filing_date": "2019-06-22",
        "inventor_id": "INV002",
        "name": "Kenji Tanaka",
        "country": "JP",
        "assignee_id": "CMP002",
        "company_name": "NexFab Systems",
    },
    {
        "patent_id": "US1000003",
        "title": "Low-latency network routing",
        "abstract": "Adaptive path computation for reduced network latency.",
        "filing_date": "2020-02-15",
        "inventor_id": "INV001",
        "name": "Alicia Gomez",
        "country": "US",
        "assignee_id": "CMP001",
        "company_name": "Voltix Labs",
    },
    {
        "patent_id": "US1000004",
        "title": "Genome pattern detection",
        "abstract": "Parallel algorithm for gene sequence anomaly detection.",
        "filing_date": "2021-11-03",
        "inventor_id": "INV003",
        "name": "Sofia Martins",
        "country": "BR",
        "assignee_id": "CMP003",
        "company_name": "BioNova Analytics",
    },
]


def snake_case(column_name: str) -> str:
    """Convert column names into snake_case."""
    cleaned = re.sub(r"[^\w]+", "_", column_name.strip())
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    return cleaned.strip("_").lower()


def detect_separator(file_path: Path) -> str:
    """Infer delimiter from file extension."""
    if file_path.suffix.lower() in {".tsv", ".txt"}:
        return "\t"
    return ","


def ensure_raw_data(raw_dir: Path) -> None:
    """Create deterministic sample raw data when no source files exist."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    if any(raw_dir.glob("*.csv")) or any(raw_dir.glob("*.tsv")):
        return

    sample_file = raw_dir / "sample_patents.csv"
    pd.DataFrame(SAMPLE_DATA).to_csv(sample_file, index=False)


def list_raw_files(raw_dir: Path) -> Iterable[Path]:
    """List supported raw files."""
    return sorted(
        [
            *raw_dir.glob("*.csv"),
            *raw_dir.glob("*.tsv"),
            *raw_dir.glob("*.txt"),
        ]
    )


def map_relevant_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select and standardize only relevant fields from the raw input."""
    normalized_columns = {col: snake_case(col) for col in df.columns}
    df = df.rename(columns=normalized_columns)

    selected = pd.DataFrame(index=df.index)
    for canonical_name, aliases in RELEVANT_FIELDS.items():
        match = next((alias for alias in aliases if alias in df.columns), None)
        selected[canonical_name] = df.get(match, pd.NA)

    return selected


def extract_from_file(file_path: Path) -> pd.DataFrame:
    """Extract relevant patent attributes from one file."""
    sep = detect_separator(file_path)
    raw_df = pd.read_csv(file_path, sep=sep, dtype=str)
    extracted_df = map_relevant_columns(raw_df)
    extracted_df["source_file"] = file_path.name
    return extracted_df


def run_extraction(raw_dir: Path = DEFAULT_RAW_DIR, output_file: Path = DEFAULT_OUTPUT_FILE) -> Path:
    """Run extraction from all available raw files and persist unified extracted data."""
    raw_dir = Path(raw_dir)
    output_file = Path(output_file)

    ensure_raw_data(raw_dir)
    files = list_raw_files(raw_dir)

    if not files:
        raise FileNotFoundError(f"No raw input files found in {raw_dir}.")

    extracted_frames = [extract_from_file(path) for path in files]
    extracted = pd.concat(extracted_frames, ignore_index=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    extracted.to_csv(output_file, index=False)
    return output_file


if __name__ == "__main__":
    destination = run_extraction()
    print(f"Extracted records saved to {destination}")
