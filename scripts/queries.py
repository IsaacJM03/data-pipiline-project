"""Run analytical SQL queries and return result dataframes."""

from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd

from scripts.logging_config import get_logger, log_pipeline_start, log_pipeline_end, log_stats

logger = get_logger("queries")

DEFAULT_DB_PATH = Path("data/patent_analytics.db")
DEFAULT_QUERIES_PATH = Path("sql/queries.sql")
DEFAULT_EXPORTS_DIR = Path("outputs/exports")


def parse_named_queries(queries_path: Path) -> dict[str, str]:
    """Parse named SQL queries from a SQL file.

    Expected format:
    -- name: query_key
    SELECT ...;
    """
    logger.debug(f"[Query Parsing] Reading queries from {queries_path}")
    query_map: dict[str, list[str]] = {}
    current_key: str | None = None

    for line in queries_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            current_key = stripped.split(":", 1)[1].strip()
            query_map[current_key] = []
            continue
        if current_key is not None:
            query_map[current_key].append(line)

    parsed = {
        key: "\n".join(lines).strip().rstrip(";")
        for key, lines in query_map.items()
        if "\n".join(lines).strip()
    }
    
    logger.info(f"[Query Parsing] Parsed {len(parsed)} named queries")
    for query_name in parsed:
        logger.debug(f"[Query Parsing]   - {query_name}")
    
    return parsed


def run_queries(
    db_path: Path = DEFAULT_DB_PATH,
    queries_path: Path = DEFAULT_QUERIES_PATH,
    exports_dir: Path = DEFAULT_EXPORTS_DIR,
) -> dict[str, pd.DataFrame]:
    """Execute all named analytical SQL queries."""
    log_pipeline_start("QUERIES STAGE")
    
    db_path = Path(db_path)
    queries_path = Path(queries_path)
    exports_dir = Path(exports_dir)

    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        raise FileNotFoundError(f"Database file not found: {db_path}")
    if not queries_path.exists():
        logger.error(f"Queries file not found: {queries_path}")
        raise FileNotFoundError(f"Queries file not found: {queries_path}")

    logger.info(f"[Database] Loading database: {db_path}")
    queries = parse_named_queries(queries_path)
    results: dict[str, pd.DataFrame] = {}
    query_stats = {}

    with sqlite3.connect(db_path) as conn:
        for idx, (query_name, sql) in enumerate(queries.items(), 1):
            logger.info(f"[Query Execution] Executing query {idx}/{len(queries)}: {query_name}")
            logger.debug(f"[Query SQL] {query_name}: {sql[:100]}..." if len(sql) > 100 else f"[Query SQL] {query_name}: {sql}")
            try:
                results[query_name] = pd.read_sql_query(sql, conn)
                row_count = len(results[query_name])
                col_count = len(results[query_name].columns)
                query_stats[query_name] = row_count
                logger.info(f"[Query Result] {query_name}: {row_count:,} rows x {col_count} columns")
            except Exception as e:
                logger.error(f"[Query Error] {query_name}: {e}")
                raise

    logger.info(f"[Export] Writing {len(results)} query results to CSV...")
    exports_dir.mkdir(parents=True, exist_ok=True)
    for query_name, dataframe in results.items():
        export_path = exports_dir / f"{query_name}.csv"
        dataframe.to_csv(export_path, index=False)
        logger.info(f"[Export] {query_name}: {export_path}")

    log_stats("QUERIES STAGE SUMMARY", query_stats)
    log_pipeline_end("QUERIES STAGE", "SUCCESS")
    
    return results


if __name__ == "__main__":
    query_results = run_queries()
    for query_name, dataframe in query_results.items():
        print(f"{query_name}: {len(dataframe)} rows")
