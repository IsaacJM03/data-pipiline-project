"""Extract stage for USPTO PatentsView-like raw files."""

from __future__ import annotations

from html import unescape
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import TYPE_CHECKING, Iterable
from urllib.parse import urljoin, urlsplit
import zipfile

if TYPE_CHECKING:
    import pandas as pd

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_OUTPUT_FILE = Path("data/processed/extracted_patent_records.csv")
PATENTSVIEW_DATASET_URL = (
    "https://data.uspto.gov/bulkdata/datasets/pvgpatdis"
    "?fileDataFromDate=1976-01-01&fileDataToDate=2025-09-30"
)
PATENTSVIEW_AUTO_DOWNLOAD_ENV = "PATENTSVIEW_AUTO_DOWNLOAD"
PATENTSVIEW_MAX_DOWNLOAD_FILES_ENV = "PATENTSVIEW_MAX_DOWNLOAD_FILES"

RELEVANT_FIELDS = {
    "patent_id": ["patent_id", "patent", "id"],
    "title": ["title", "patent_title"],
    "abstract": ["abstract", "patent_abstract"],
    "filing_date": ["filing_date", "date", "patent_date"],
    "classification": [
        "classification",
        "main_classification",
        "cpc_subgroup_id",
        "cpc_group_id",
        "uspc_mainclass_id",
    ],
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
        "classification": "H01M10/0525",
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
        "classification": "G06Q10/06",
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
        "classification": "H04L45/00",
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
        "classification": "C12Q1/6869",
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
    """Populate raw folder from PatentsView dataset page, then fallback to sample data."""
    import pandas as pd

    raw_dir.mkdir(parents=True, exist_ok=True)
    if any(raw_dir.glob("*.csv")) or any(raw_dir.glob("*.tsv")):
        return

    auto_download = os.getenv(PATENTSVIEW_AUTO_DOWNLOAD_ENV, "1").lower() not in {"0", "false", "no"}
    if auto_download and pull_patentsview_data(raw_dir):
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
    import pandas as pd

    normalized_columns = {col: snake_case(col) for col in df.columns}
    df = df.rename(columns=normalized_columns)

    selected = pd.DataFrame(index=df.index)
    for canonical_name, aliases in RELEVANT_FIELDS.items():
        match = next((alias for alias in aliases if alias in df.columns), None)
        selected[canonical_name] = df.get(match, pd.NA)

    return selected


def discover_patentsview_file_links(dataset_url: str) -> list[str]:
    """Collect downloadable ZIP file links from the PatentsView dataset page."""
    html = fetch_page_with_cli(dataset_url)

    href_candidates = re.findall(r"""href=["']([^"']+)["']""", html, flags=re.IGNORECASE)
    text_candidates = re.findall(r"""https?://[^"'\s<>]+\.zip(?:\?[^"'\s<>]*)?""", html, flags=re.IGNORECASE)
    relative_candidates = re.findall(r"""/[^"'\s<>]+\.zip(?:\?[^"'\s<>]*)?""", html, flags=re.IGNORECASE)

    discovered = []
    for candidate in [*href_candidates, *text_candidates, *relative_candidates]:
        absolute_link = urljoin(dataset_url, unescape(candidate.strip()))
        link_path = urlsplit(absolute_link).path.lower()
        if link_path.endswith(".zip"):
            discovered.append(absolute_link)

    return sorted(set(discovered))


def _run_command(command: list[str], timeout_seconds: int = 180) -> str:
    """Run a subprocess command and return stdout as text."""
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    return result.stdout


def fetch_page_with_cli(url: str) -> str:
    """Fetch HTML via curl/wget, preferring curl when available."""
    if shutil.which("curl"):
        return _run_command(["curl", "-fsSL", "-L", url], timeout_seconds=120)
    if shutil.which("wget"):
        return _run_command(["wget", "-qO-", url], timeout_seconds=120)
    raise RuntimeError("Neither curl nor wget is available on PATH.")


def download_url_to_path(url: str, target_path: Path) -> None:
    """Download a URL to disk via curl/wget."""
    if shutil.which("curl"):
        subprocess.run(
            ["curl", "-fL", "--retry", "3", "-o", str(target_path), url],
            check=True,
            timeout=300,
        )
        return

    if shutil.which("wget"):
        subprocess.run(
            ["wget", "-q", "-O", str(target_path), url],
            check=True,
            timeout=300,
        )
        return

    raise RuntimeError("Neither curl nor wget is available on PATH.")


def unpack_zip_files(raw_dir: Path) -> None:
    """Extract all zip archives in the raw directory."""
    for archive in raw_dir.glob("*.zip"):
        with zipfile.ZipFile(archive) as zipped:
            zipped.extractall(raw_dir)


def pull_patentsview_data(raw_dir: Path) -> bool:
    """Try pulling source files from the official PatentsView granted dataset page."""
    try:
        max_files = int(os.getenv(PATENTSVIEW_MAX_DOWNLOAD_FILES_ENV, "8"))
    except ValueError:
        max_files = 8
    max_files = max(1, max_files)

    try:
        links = discover_patentsview_file_links(PATENTSVIEW_DATASET_URL)
        if not links:
            return False

        selected_links = links if max_files <= 0 else links[:max_files]
        for file_url in selected_links:
            file_name = Path(urlsplit(file_url).path).name
            target = raw_dir / file_name
            download_url_to_path(file_url, target)
        unpack_zip_files(raw_dir)
    except Exception as exc:
        print(f"Warning: Failed to pull PatentsView data from source ({exc}). Falling back to sample data.")
        return False

    return any(raw_dir.glob("*.csv")) or any(raw_dir.glob("*.tsv")) or any(raw_dir.glob("*.txt"))


def extract_from_file(file_path: Path) -> pd.DataFrame:
    """Extract relevant patent attributes from one file."""
    import pandas as pd

    sep = detect_separator(file_path)
    try:
        # Try standard read first
        raw_df = pd.read_csv(file_path, sep=sep, dtype=str)
    except pd.errors.ParserError:
        # If parsing fails, skip problematic lines
        print(f"⚠️  Parsing error in {file_path.name}, skipping malformed rows...")
        raw_df = pd.read_csv(file_path, sep=sep, dtype=str, on_bad_lines='skip', engine='python')
    
    if len(raw_df) == 0:
        return pd.DataFrame()  # Return empty dataframe if no rows extracted
    
    extracted_df = map_relevant_columns(raw_df)
    extracted_df["source_file"] = file_path.name
    return extracted_df


def run_extraction(raw_dir: Path = DEFAULT_RAW_DIR, output_file: Path = DEFAULT_OUTPUT_FILE) -> Path:
    """Run extraction from all available raw files and persist unified extracted data."""
    import pandas as pd

    raw_dir = Path(raw_dir)
    output_file = Path(output_file)

    ensure_raw_data(raw_dir)
    files = list_raw_files(raw_dir)

    if not files:
        raise FileNotFoundError(f"No raw input files found in {raw_dir}.")

    extracted_frames = []
    for path in files:
        print(f"  Extracting {path.name}...", end=" ")
        try:
            df = extract_from_file(path)
            if len(df) > 0:
                extracted_frames.append(df)
                print(f"✓ ({len(df)} rows)")
            else:
                print("⊘ (skipped - no valid rows)")
        except Exception as exc:
            print(f"✗ ({exc})")
    
    if not extracted_frames:
        raise ValueError("No data could be extracted from any files.")
    
    extracted = pd.concat(extracted_frames, ignore_index=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    extracted.to_csv(output_file, index=False)
    print(f"✓ Saved {len(extracted)} extracted records")
    return output_file


if __name__ == "__main__":
    destination = run_extraction()
    print(f"Extracted records saved to {destination}")
