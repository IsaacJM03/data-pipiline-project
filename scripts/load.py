"""Load transformed patent data into SQLite."""

from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd

DEFAULT_DB_PATH = Path("data/patent_analytics.db")
DEFAULT_SCHEMA_PATH = Path("sql/schema.sql")
DEFAULT_PROCESSED_DIR = Path("data/processed")


TABLE_FILES = {
    "patents": "clean_patents.csv",
    "inventors": "clean_inventors.csv",
    "companies": "clean_companies.csv",
    "relationships": "clean_relationships.csv",
}

CHUNK_SIZE = 100_000


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def initialize_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    """Create tables and indexes from schema SQL."""
    schema_sql = schema_path.read_text(encoding="utf-8")
    connection.executescript(schema_sql)


def load_table(connection: sqlite3.Connection, table_name: str, csv_path: Path) -> None:
    """Load a CSV file into a SQLite table using raw SQL to avoid variable limits."""
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=CHUNK_SIZE):
        # Drop duplicates within each chunk to handle any schema PK/UNIQUE constraints
        if "inventor_id" in chunk.columns:
            chunk = chunk.drop_duplicates(subset=["inventor_id"])
        if "company_id" in chunk.columns:
            chunk = chunk.drop_duplicates(subset=["company_id"])
        if "patent_id" in chunk.columns and "inventor_id" in chunk.columns and "company_id" in chunk.columns:
            chunk = chunk.drop_duplicates(subset=["patent_id", "inventor_id", "company_id"])
        
        if chunk.empty:
            continue
        
        # Use raw SQL INSERT to avoid pandas' chunksize parameter issues
        columns = list(chunk.columns)
        placeholders = ", ".join(["?" for _ in columns])
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        insert_stmt = f"INSERT INTO {_quote_identifier(table_name)} ({quoted_columns}) VALUES ({placeholders})"
        
        # Convert DataFrame rows to tuples of values
        rows_as_tuples = [tuple(row) for row in chunk.values]
        connection.executemany(insert_stmt, rows_as_tuples)


def run_load(
    db_path: Path = DEFAULT_DB_PATH,
    schema_path: Path = DEFAULT_SCHEMA_PATH,
    processed_dir: Path = DEFAULT_PROCESSED_DIR,
) -> Path:
    """Run load stage for all transformed tables."""
    db_path = Path(db_path)
    schema_path = Path(schema_path)
    processed_dir = Path(processed_dir)

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")

    for table, file_name in TABLE_FILES.items():
        csv_path = processed_dir / file_name
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing transformed file for {table}: {csv_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        initialize_schema(conn, schema_path)
        
        # Disable foreign keys during load to allow flexible insert order
        conn.execute("PRAGMA foreign_keys = OFF")
        
        for table, file_name in TABLE_FILES.items():
            load_table(conn, table, processed_dir / file_name)
            conn.commit()
        
        # Re-enable foreign keys and validate data integrity
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()

    return db_path


if __name__ == "__main__":
    database = run_load()
    print(f"Loaded data into {database}")
