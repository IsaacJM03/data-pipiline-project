"""Run analytical SQL queries and return result dataframes."""

from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd

DEFAULT_DB_PATH = Path("data/patent_analytics.db")
DEFAULT_QUERIES_PATH = Path("sql/queries.sql")
DEFAULT_EXPORTS_DIR = Path("outputs/exports")


def parse_named_queries(queries_path: Path) -> dict[str, str]:
    """Parse named SQL queries from a SQL file.

    Expected format:
    -- name: query_key
    SELECT ...;
    """
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

    return {
        key: "\n".join(lines).strip().rstrip(";")
        for key, lines in query_map.items()
        if "\n".join(lines).strip()
    }


def run_queries(
    db_path: Path = DEFAULT_DB_PATH,
    queries_path: Path = DEFAULT_QUERIES_PATH,
    exports_dir: Path = DEFAULT_EXPORTS_DIR,
) -> dict[str, pd.DataFrame]:
    """Execute all named analytical SQL queries."""
    db_path = Path(db_path)
    queries_path = Path(queries_path)
    exports_dir = Path(exports_dir)

    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")
    if not queries_path.exists():
        raise FileNotFoundError(f"Queries file not found: {queries_path}")

    queries = parse_named_queries(queries_path)
    results: dict[str, pd.DataFrame] = {}

    with sqlite3.connect(db_path) as conn:
        for query_name, sql in queries.items():
            results[query_name] = pd.read_sql_query(sql, conn)

    exports_dir.mkdir(parents=True, exist_ok=True)
    for query_name, dataframe in results.items():
        dataframe.to_csv(exports_dir / f"{query_name}.csv", index=False)

    return results


if __name__ == "__main__":
    query_results = run_queries()
    for query_name, dataframe in query_results.items():
        print(f"{query_name}: {len(dataframe)} rows")
