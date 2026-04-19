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


def initialize_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    """Create tables and indexes from schema SQL."""
    schema_sql = schema_path.read_text(encoding="utf-8")
    connection.executescript(schema_sql)


def load_table(connection: sqlite3.Connection, table_name: str, csv_path: Path) -> None:
    """Load a CSV file into a SQLite table."""
    dataframe = pd.read_csv(csv_path)
    dataframe.to_sql(table_name, connection, if_exists="append", index=False)


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
        for table, file_name in TABLE_FILES.items():
            load_table(conn, table, processed_dir / file_name)
        conn.commit()

    return db_path


if __name__ == "__main__":
    database = run_load()
    print(f"Loaded data into {database}")
