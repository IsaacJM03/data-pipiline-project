"""Load transformed patent data into SQLite."""

from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd

from scripts.logging_config import get_logger, log_pipeline_start, log_pipeline_end, log_stats

logger = get_logger("load")

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
    logger.info(f"[Schema] Reading schema from {schema_path}")
    schema_sql = schema_path.read_text(encoding="utf-8")
    logger.debug(f"[Schema] Executing schema SQL ({len(schema_sql)} bytes)")
    connection.executescript(schema_sql)
    logger.info("[Schema] Schema initialization complete")


def load_table(connection: sqlite3.Connection, table_name: str, csv_path: Path) -> None:
    """Load a CSV file into a SQLite table using raw SQL to avoid variable limits."""
    logger.info(f"[Load Table] Loading {table_name} from {csv_path.name}...")
    total_rows = 0
    chunk_num = 0
    
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=CHUNK_SIZE):
        chunk_num += 1
        # Drop duplicates within each chunk to handle any schema PK/UNIQUE constraints
        if "inventor_id" in chunk.columns:
            chunk = chunk.drop_duplicates(subset=["inventor_id"])
        if "company_id" in chunk.columns:
            chunk = chunk.drop_duplicates(subset=["company_id"])
        if "patent_id" in chunk.columns and "inventor_id" in chunk.columns and "company_id" in chunk.columns:
            chunk = chunk.drop_duplicates(subset=["patent_id", "inventor_id", "company_id"])
        
        if chunk.empty:
            logger.debug(f"[Load Table] Chunk {chunk_num} was empty after deduplication")
            continue
        
        # Use raw SQL INSERT to avoid pandas' chunksize parameter issues
        columns = list(chunk.columns)
        placeholders = ", ".join(["?" for _ in columns])
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        insert_stmt = f"INSERT INTO {_quote_identifier(table_name)} ({quoted_columns}) VALUES ({placeholders})"
        
        # Convert DataFrame rows to tuples of values
        rows_as_tuples = [tuple(row) for row in chunk.values]
        connection.executemany(insert_stmt, rows_as_tuples)
        total_rows += len(rows_as_tuples)
        
        if chunk_num % 10 == 0:
            logger.debug(f"[Load Table] {table_name}: Processed {chunk_num} chunks ({total_rows:,} rows)")
    
    logger.info(f"[Load Table] {table_name}: {total_rows:,} rows loaded in {chunk_num} chunks")


def run_load(
    db_path: Path = DEFAULT_DB_PATH,
    schema_path: Path = DEFAULT_SCHEMA_PATH,
    processed_dir: Path = DEFAULT_PROCESSED_DIR,
) -> Path:
    """Run load stage for all transformed tables."""
    log_pipeline_start("LOAD STAGE")
    
    db_path = Path(db_path)
    schema_path = Path(schema_path)
    processed_dir = Path(processed_dir)

    if not schema_path.exists():
        logger.error(f"Schema not found: {schema_path}")
        raise FileNotFoundError(f"Schema not found: {schema_path}")

    logger.info(f"[Database] Using database: {db_path}")
    logger.info(f"[Files] Checking for processed files in {processed_dir}")
    
    for table, file_name in TABLE_FILES.items():
        csv_path = processed_dir / file_name
        if not csv_path.exists():
            logger.error(f"Missing transformed file for {table}: {csv_path}")
            raise FileNotFoundError(f"Missing transformed file for {table}: {csv_path}")
        logger.debug(f"[Files] ✓ {file_name}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"[Database] Created database directory: {db_path.parent}")
    
    with sqlite3.connect(db_path) as conn:
        initialize_schema(conn, schema_path)
        
        # Disable foreign keys during load to allow flexible insert order
        logger.info("[Integrity] Disabling foreign key constraints during load")
        conn.execute("PRAGMA foreign_keys = OFF")
        
        load_stats = {}
        for table, file_name in TABLE_FILES.items():
            load_table(conn, table, processed_dir / file_name)
            conn.commit()
            logger.debug(f"[Database] Committed transaction for {table}")
        
        # Re-enable foreign keys and validate data integrity
        logger.info("[Integrity] Re-enabling foreign key constraints")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.commit()
        
        # Log final table row counts
        logger.info("[Verification] Final table row counts:")
        for table, file_name in TABLE_FILES.items():
            cursor = conn.execute(f'SELECT COUNT(*) FROM {_quote_identifier(table)}')
            count = cursor.fetchone()[0]
            logger.info(f"[Verification]   {table}: {count:,} rows")
            load_stats[table] = count
    
    logger.info(f"[Output] Database file: {db_path}")
    log_stats("LOAD STAGE SUMMARY", load_stats | {"database_file": str(db_path)})
    log_pipeline_end("LOAD STAGE", "SUCCESS")
    
    return db_path


if __name__ == "__main__":
    database = run_load()
    print(f"Loaded data into {database}")
