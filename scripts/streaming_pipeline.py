"""Streaming ETL for the patent analytics pipeline."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
STAGING_DB = PROCESSED_DIR / "patent_staging.db"
CHUNK_SIZE = int(os.getenv("PATENT_PIPELINE_CHUNK_SIZE", "100000"))
ROW_LIMIT_ENV = "PATENT_PIPELINE_MAX_ROWS"

CORE_SOURCE_FILES = {
    "patents_raw": RAW_DIR / "g_patent.tsv",
    "abstracts_raw": RAW_DIR / "g_patent_abstract.tsv",
    "inventors_raw": RAW_DIR / "g_inventor_disambiguated.tsv",
    "locations_raw": RAW_DIR / "g_location_disambiguated.tsv",
    # Use the persistent assignee table for company UUID columns (disamb_assignee_id_*)
    # The disambiguated assignee file (`g_assignee_disambiguated.tsv`) contains organization
    # names and `assignee_id`, while the persistent assignee file contains the
    # `disamb_assignee_id_YYYYMMDD` columns we need to resolve companies.
    "assignees_raw": RAW_DIR / "g_persistent_assignee.tsv",
}

# Optional non-disambiguated locations to fill missing country values
OPTIONAL_SOURCE_FILES = {
    "locations_not_disambiguated_raw": RAW_DIR / "g_location_not_disambiguated.tsv",
}

def _normalize_country_code(raw_country: str | None) -> str | None:
    """Normalize raw country string to ISO 3166-1 alpha-2 code.
    
    Uses a small heuristic map to handle common misspellings and variants.
    """
    if not raw_country or not isinstance(raw_country, str):
        return None
    
    country = raw_country.strip().upper()
    if not country:
        return None
    
    # Map of known variants to ISO codes
    normalize_map = {
        "US": "US", "USA": "US", "UNITED STATES": "US", "UNITED STATES OF AMERICA": "US",
        "GB": "GB", "UK": "GB", "UNITED KINGDOM": "GB", "GREAT BRITAIN": "GB",
        "JP": "JP", "JAPAN": "JP",
        "DE": "DE", "GERMANY": "DE", "DEUTSCHLAND": "DE",
        "FR": "FR", "FRANCE": "FR",
        "CA": "CA", "CANADA": "CA",
        "CN": "CN", "CHINA": "CN",
        "IN": "IN", "INDIA": "IN",
        "BR": "BR", "BRAZIL": "BR",
        "AU": "AU", "AUSTRALIA": "AU",
        "NL": "NL", "NETHERLANDS": "NL", "HOLLAND": "NL",
        "KR": "KR", "SOUTH KOREA": "KR", "KOREA": "KR",
        "SG": "SG", "SINGAPORE": "SG",
        "CH": "CH", "SWITZERLAND": "CH", "SWISS": "CH",
        "SE": "SE", "SWEDEN": "SE",
        "NO": "NO", "NORWAY": "NO",
        "DK": "DK", "DENMARK": "DK",
        "IT": "IT", "ITALY": "IT",
        "ES": "ES", "SPAIN": "ES",
        "MX": "MX", "MEXICO": "MX",
        "RU": "RU", "RUSSIA": "RU", "RUSSIAN FEDERATION": "RU",
        "IL": "IL", "ISRAEL": "IL",
        "TW": "TW", "TAIWAN": "TW",
        "HK": "HK", "HONG KONG": "HK",
    }
    
    # Try exact match first
    if country in normalize_map:
        return normalize_map[country]
    
    # Try first 2 chars if it's already a code-like format
    if len(country) >= 2 and country[:2].isalpha():
        return country[:2]
    
    return None

# Optional assignee names file for enriching company names with disambiguated organization field
ASSIGNEE_NAMES_FILE = RAW_DIR / "g_assignee_disambiguated.tsv"

SAMPLE_PATENTS = pd.DataFrame(
    [
        {
            "patent_id": "US1000001",
            "title": "Battery cooling optimization",
            "abstract": "System and method for electric battery thermal regulation.",
            "filing_date": "2018-04-10",
            "year": 2018,
            "classification": "H01M10/0525",
        },
        {
            "patent_id": "US1000002",
            "title": "AI scheduling for manufacturing",
            "abstract": "Machine learning model to optimize factory throughput.",
            "filing_date": "2019-06-22",
            "year": 2019,
            "classification": "G06Q10/06",
        },
        {
            "patent_id": "US1000003",
            "title": "Low-latency network routing",
            "abstract": "Adaptive path computation for reduced network latency.",
            "filing_date": "2020-02-15",
            "year": 2020,
            "classification": "H04L45/00",
        },
        {
            "patent_id": "US1000004",
            "title": "Genome pattern detection",
            "abstract": "Parallel algorithm for gene sequence anomaly detection.",
            "filing_date": "2021-11-03",
            "year": 2021,
            "classification": "C12Q1/6869",
        },
    ]
)
SAMPLE_INVENTORS = pd.DataFrame(
    [
        {"inventor_id": "INV001", "name": "Alicia Gomez", "country": "US"},
        {"inventor_id": "INV002", "name": "Kenji Tanaka", "country": "JP"},
        {"inventor_id": "INV003", "name": "Sofia Martins", "country": "BR"},
    ]
)
SAMPLE_COMPANIES = pd.DataFrame(
    [
        {"company_id": "CMP001", "name": "Voltix Labs"},
        {"company_id": "CMP002", "name": "NexFab Systems"},
        {"company_id": "CMP003", "name": "BioNova Analytics"},
    ]
)
SAMPLE_RELATIONSHIPS = pd.DataFrame(
    [
        {"patent_id": "US1000001", "inventor_id": "INV001", "company_id": "CMP001"},
        {"patent_id": "US1000002", "inventor_id": "INV002", "company_id": "CMP002"},
        {"patent_id": "US1000003", "inventor_id": "INV001", "company_id": "CMP001"},
        {"patent_id": "US1000004", "inventor_id": "INV003", "company_id": "CMP003"},
    ]
)


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [column.strip().strip('"').lower() for column in frame.columns]
    return frame


def _core_files_available(raw_dir: Path) -> bool:
    return all((raw_dir / source_file.name).exists() for source_file in CORE_SOURCE_FILES.values())


def _chunk_limit() -> int | None:
    value = os.getenv(ROW_LIMIT_ENV)
    if not value:
        return None
    try:
        limit = int(value)
    except ValueError:
        return None
    return max(limit, 0)


def _iter_tsv_chunks(file_path: Path, usecols: list[str] | None = None) -> Iterable[pd.DataFrame]:
    kwargs: dict[str, object] = {
        "sep": "\t",
        "dtype": str,
        "on_bad_lines": "skip",
        "engine": "python",
        "chunksize": CHUNK_SIZE,
    }
    if usecols is not None:
        kwargs["usecols"] = usecols

    remaining = _chunk_limit()
    for chunk in pd.read_csv(file_path, **kwargs):
        if remaining is not None:
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk.head(remaining)
            remaining -= len(chunk)
        yield _normalize_columns(chunk)


def _configure_sqlite(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode = OFF")
    connection.execute("PRAGMA synchronous = OFF")
    connection.execute("PRAGMA temp_store = MEMORY")
    connection.execute("PRAGMA cache_size = -200000")


def _append_chunks_to_table(
    connection: sqlite3.Connection,
    table_name: str,
    chunks: Iterable[pd.DataFrame],
    transform: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> int:
    written_rows = 0
    SQLITE_MAX_VARS = 999
    table_created = False
    transformed_columns = None
    
    for chunk in chunks:
        if transform is not None:
            chunk = transform(chunk)
            # Capture transformed output columns (even from empty DataFrames)
            if transformed_columns is None:
                transformed_columns = list(chunk.columns)
            
        if chunk.empty:
            continue

        # Create table on first non-empty chunk
        if not table_created:
            connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            cols_sql = ", ".join(f'"{col}" TEXT' for col in chunk.columns)
            connection.execute(f'CREATE TABLE "{table_name}" ({cols_sql})')
            table_created = True
        
        # Split chunk into smaller sub-chunks to avoid SQLite "too many SQL variables"
        # Use a conservative limit: max 50 rows per insert to be safe
        max_rows_per_insert = min(50, max(1, SQLITE_MAX_VARS // max(1, len(chunk.columns))))

        for start in range(0, len(chunk), max_rows_per_insert):
            sub = chunk.iloc[start : start + max_rows_per_insert]
            if sub.empty:
                continue
            
            # Use raw SQL INSERT to avoid pandas' chunksize parameter issues
            columns = list(sub.columns)
            placeholders = ", ".join(["?" for _ in columns])
            insert_stmt = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Convert DataFrame rows to tuples of values
            rows_as_tuples = [tuple(row) for row in sub.values]
            connection.executemany(insert_stmt, rows_as_tuples)
            written_rows += len(sub)
    
    # If no data was written but we know the schema, create an empty table
    if not table_created and transformed_columns:
        connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cols_sql = ", ".join(f'"{col}" TEXT' for col in transformed_columns)
        connection.execute(f'CREATE TABLE "{table_name}" ({cols_sql})')
        table_created = True
    
    connection.commit()
    return written_rows


def _load_patents(chunk: pd.DataFrame) -> pd.DataFrame:
    result = chunk[["patent_id", "patent_date", "patent_title", "wipo_kind", "patent_type"]].copy()
    result = result.rename(columns={"patent_title": "title"})
    result = result.dropna(subset=["patent_id"])
    return result


def _load_abstracts(chunk: pd.DataFrame) -> pd.DataFrame:
    result = chunk[["patent_id", "patent_abstract"]].copy()
    result = result.rename(columns={"patent_abstract": "abstract"})
    result = result.dropna(subset=["patent_id"])
    return result


def _load_inventors(chunk: pd.DataFrame) -> pd.DataFrame:
    result = chunk[["patent_id", "inventor_id", "disambig_inventor_name_first", "disambig_inventor_name_last", "location_id"]].copy()
    result = result.dropna(subset=["patent_id", "inventor_id"])
    return result


def _load_locations(chunk: pd.DataFrame) -> pd.DataFrame:
    result = chunk[["location_id", "disambig_country"]].copy()
    result = result.dropna(subset=["location_id"])
    return result


def _load_locations_not_disambiguated(chunk: pd.DataFrame) -> pd.DataFrame:
    """Load non-disambiguated locations and normalize raw_country codes."""
    result = chunk[["location_id", "raw_country"]].copy()
    result = result.rename(columns={"raw_country": "country"})
    result["country"] = result["country"].apply(_normalize_country_code)
    result = result.dropna(subset=["location_id", "country"])
    return result


def _load_assignees(chunk: pd.DataFrame) -> pd.DataFrame:
    company_columns = [column for column in chunk.columns if column.startswith("disamb_assignee_id_")]
    if not company_columns:
        return pd.DataFrame(columns=["patent_id", "company_id"])

    result = chunk[["patent_id", *company_columns]].copy()
    result[company_columns] = result[company_columns].replace("", pd.NA)
    result["company_id"] = result[company_columns].bfill(axis=1).iloc[:, 0]
    result = result[["patent_id", "company_id"]].dropna(subset=["patent_id", "company_id"])
    return result


def _load_assignee_names(chunk: pd.DataFrame) -> pd.DataFrame:
    id_col = next((c for c in ("assignee_id", "disambig_assignee_id") if c in chunk.columns), None)
    if not id_col or "disambig_assignee_organization" not in chunk.columns:
        return pd.DataFrame(columns=["assignee_id", "organization"])

    result = chunk[[id_col, "disambig_assignee_organization"]].copy()
    result = result.rename(columns={id_col: "assignee_id", "disambig_assignee_organization": "organization"})
    result = result.dropna(subset=["assignee_id", "organization"])
    result = result[result["organization"].str.strip() != ""]
    return result


def _write_query_to_csv(connection: sqlite3.Connection, query: str, output_path: Path, chunksize: int = 100_000) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    first_chunk = True
    for chunk in pd.read_sql_query(query, connection, chunksize=chunksize):
        if first_chunk:
            # Write first chunk with header using mode='w' to ensure header is written
            chunk.to_csv(output_path, index=False, mode="w", header=True)
            first_chunk = False
        else:
            # Append subsequent chunks without header using mode='a'
            chunk.to_csv(output_path, index=False, mode="a", header=False)

    # If no chunks were written (empty result), create an empty file with headers only
    if first_chunk:
        pd.DataFrame().to_csv(output_path, index=False)


def _write_sample_outputs(processed_dir: Path) -> dict[str, Path]:
    processed_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "patents": processed_dir / "clean_patents.csv",
        "inventors": processed_dir / "clean_inventors.csv",
        "companies": processed_dir / "clean_companies.csv",
        "relationships": processed_dir / "clean_relationships.csv",
    }
    SAMPLE_PATENTS.to_csv(outputs["patents"], index=False)
    SAMPLE_INVENTORS.to_csv(outputs["inventors"], index=False)
    SAMPLE_COMPANIES.to_csv(outputs["companies"], index=False)
    SAMPLE_RELATIONSHIPS.to_csv(outputs["relationships"], index=False)
    return outputs


def build_staging_database(raw_dir: Path = RAW_DIR, staging_db: Path = STAGING_DB) -> Path:
    raw_dir = Path(raw_dir)
    staging_db = Path(staging_db)

    if not _core_files_available(raw_dir):
        raise FileNotFoundError("Missing one or more required core USPTO TSV files in data/raw.")

    staging_db.parent.mkdir(parents=True, exist_ok=True)
    if staging_db.exists():
        staging_db.unlink()

    with sqlite3.connect(staging_db) as connection:
        _configure_sqlite(connection)

        _append_chunks_to_table(
            connection,
            "patents_raw",
            _iter_tsv_chunks(CORE_SOURCE_FILES["patents_raw"]),
            _load_patents,
        )
        _append_chunks_to_table(
            connection,
            "abstracts_raw",
            _iter_tsv_chunks(CORE_SOURCE_FILES["abstracts_raw"]),
            _load_abstracts,
        )
        _append_chunks_to_table(
            connection,
            "inventors_raw",
            _iter_tsv_chunks(CORE_SOURCE_FILES["inventors_raw"]),
            _load_inventors,
        )
        _append_chunks_to_table(
            connection,
            "locations_raw",
            _iter_tsv_chunks(CORE_SOURCE_FILES["locations_raw"]),
            _load_locations,
        )

        assignee_header = pd.read_csv(CORE_SOURCE_FILES["assignees_raw"], sep="\t", nrows=0, engine="python")
        assignee_usecols = ["patent_id", *[column for column in assignee_header.columns if column.startswith("disamb_assignee_id_")]]
        _append_chunks_to_table(
            connection,
            "assignees_raw",
            _iter_tsv_chunks(CORE_SOURCE_FILES["assignees_raw"], usecols=assignee_usecols),
            _load_assignees,
        )

        if ASSIGNEE_NAMES_FILE.exists():
            _append_chunks_to_table(
                connection,
                "assignee_names_raw",
                _iter_tsv_chunks(
                    ASSIGNEE_NAMES_FILE,
                    usecols=["assignee_id", "disambig_assignee_organization"],
                ),
                _load_assignee_names,
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_assignee_names_id ON assignee_names_raw(assignee_id)"
            )

        connection.execute("CREATE INDEX IF NOT EXISTS idx_patents_raw_id ON patents_raw(patent_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_abstracts_raw_id ON abstracts_raw(patent_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_inventors_raw_id ON inventors_raw(patent_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_locations_raw_id ON locations_raw(location_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_assignees_raw_id ON assignees_raw(company_id)")

        # Optional: load non-disambiguated locations for country fallback
        locations_not_disambig = OPTIONAL_SOURCE_FILES["locations_not_disambiguated_raw"]
        if locations_not_disambig.exists():
            _append_chunks_to_table(
                connection,
                "locations_not_disambiguated_raw",
                _iter_tsv_chunks(locations_not_disambig, usecols=["location_id", "raw_country"]),
                _load_locations_not_disambiguated,
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_locations_not_disamb_id ON locations_not_disambiguated_raw(location_id)")
        
        connection.commit()

    return staging_db


def materialize_clean_tables(staging_db: Path, processed_dir: Path = PROCESSED_DIR) -> dict[str, Path]:
    staging_db = Path(staging_db)
    processed_dir = Path(processed_dir)

    if not staging_db.exists():
        raise FileNotFoundError(f"Missing staging database: {staging_db}")

    outputs = {
        "patents": processed_dir / "clean_patents.csv",
        "inventors": processed_dir / "clean_inventors.csv",
        "companies": processed_dir / "clean_companies.csv",
        "relationships": processed_dir / "clean_relationships.csv",
    }
    processed_dir.mkdir(parents=True, exist_ok=True)
    for output in outputs.values():
        if output.exists():
            output.unlink()

    with sqlite3.connect(staging_db) as connection:
        # Patents: use GROUP BY to ensure one row per patent_id
        _write_query_to_csv(
            connection,
            """
            SELECT
                p.patent_id,
                MAX(p.title) AS title,
                MAX(a.abstract) AS abstract,
                MAX(p.patent_date) AS filing_date,
                CAST(strftime('%Y', MAX(p.patent_date)) AS INTEGER) AS year,
                MAX(COALESCE(NULLIF(p.wipo_kind, ''), NULLIF(p.patent_type, ''))) AS classification
            FROM patents_raw p
            LEFT JOIN abstracts_raw a
                ON a.patent_id = p.patent_id
            WHERE p.patent_id IS NOT NULL AND p.patent_id <> ''
            GROUP BY p.patent_id
            ORDER BY p.patent_id
            """,
            outputs["patents"],
        )

        # Inventors: use GROUP BY to ensure one row per inventor_id
        # Use COALESCE to prefer disambiguated country, fall back to normalized raw_country
        _write_query_to_csv(
            connection,
            """
            SELECT
                i.inventor_id,
                MAX(COALESCE(NULLIF(TRIM(COALESCE(i.disambig_inventor_name_first, '') || ' ' || COALESCE(i.disambig_inventor_name_last, '')), ''), i.inventor_id)) AS name,
                MAX(COALESCE(l.disambig_country, lnd.country)) AS country
            FROM inventors_raw i
            LEFT JOIN locations_raw l
                ON l.location_id = i.location_id
            LEFT JOIN locations_not_disambiguated_raw lnd
                ON lnd.location_id = i.location_id
            WHERE i.inventor_id IS NOT NULL AND i.inventor_id <> ''
            GROUP BY i.inventor_id
            ORDER BY i.inventor_id
            """,
            outputs["inventors"],
        )

        # Companies: resolve UUIDs to real org names when g_assignee_disambiguated.tsv was loaded
        _write_query_to_csv(
            connection,
            """
            SELECT DISTINCT
                a.company_id,
                COALESCE(
                    NULLIF(TRIM(COALESCE(n.organization, '')), ''),
                    a.company_id
                ) AS name
            FROM assignees_raw a
            LEFT JOIN assignee_names_raw n ON n.assignee_id = a.company_id
            WHERE a.company_id IS NOT NULL AND a.company_id <> ''
            ORDER BY a.company_id
            """,
            outputs["companies"],
        )

        # Relationships: use GROUP BY to ensure one row per (patent_id, inventor_id, company_id) tuple
        _write_query_to_csv(
            connection,
            """
            SELECT
                i.patent_id,
                i.inventor_id,
                a.company_id
            FROM inventors_raw i
            INNER JOIN assignees_raw a
                ON a.patent_id = i.patent_id
            WHERE i.patent_id IS NOT NULL
              AND i.inventor_id IS NOT NULL
              AND a.company_id IS NOT NULL
            GROUP BY i.patent_id, i.inventor_id, a.company_id
            ORDER BY i.patent_id, i.inventor_id, a.company_id
            """,
            outputs["relationships"],
        )

    return outputs


def run_streaming_pipeline(raw_dir: Path = RAW_DIR, processed_dir: Path = PROCESSED_DIR) -> dict[str, Path]:
    """Build clean patent tables from the raw USPTO TSV files."""
    try:
        staging_db = build_staging_database(raw_dir=raw_dir, staging_db=Path(processed_dir) / STAGING_DB.name)
        return materialize_clean_tables(staging_db=staging_db, processed_dir=processed_dir)
    except FileNotFoundError as exc:
        print(f"Warning: {exc}. Falling back to the bundled sample dataset.")
        return _write_sample_outputs(Path(processed_dir))
