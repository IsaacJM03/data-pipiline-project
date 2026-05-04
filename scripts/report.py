"""Generate console, CSV, JSON, and chart reports for patent analytics."""

from __future__ import annotations

from pathlib import Path
import json
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd

from scripts.queries import run_queries

DEFAULT_DB_PATH = Path("data/patent_analytics.db")
DEFAULT_REPORTS_DIR = Path("outputs/reports")
DEFAULT_EXPORTS_DIR = Path("outputs/exports")


def _to_records(df: pd.DataFrame, limit: int | None = None) -> list[dict]:
    if limit is not None:
        df = df.head(limit)
    return df.to_dict(orient="records")


def generate_visualizations(results: dict[str, pd.DataFrame], reports_dir: Path) -> Path:
    """Create a simple patents-per-year trend chart."""
    trend_df = results.get("q4_patents_per_year", pd.DataFrame())
    chart_path = reports_dir / "patents_per_year.png"

    if trend_df.empty:
        return chart_path

    # Ensure year is numeric and handle any indexing issues
    trend_df = trend_df.reset_index(drop=True)
    trend_df["year"] = pd.to_numeric(trend_df["year"], errors="coerce")
    trend_df = trend_df.dropna(subset=["year"])
    
    if trend_df.empty:
        return chart_path

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(trend_df["year"], trend_df["patent_count"], marker="o")
    ax.set_title("Patents per Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Patent Count")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(chart_path)
    plt.close(fig)

    return chart_path


def generate_reports(
    db_path: Path = DEFAULT_DB_PATH,
    reports_dir: Path = DEFAULT_REPORTS_DIR,
    exports_dir: Path = DEFAULT_EXPORTS_DIR,
) -> dict:
    """Generate required outputs and return JSON payload."""
    db_path = Path(db_path)
    reports_dir = Path(reports_dir)
    exports_dir = Path(exports_dir)

    reports_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)

    results = run_queries(db_path=db_path, exports_dir=exports_dir)
    total_patents_sql = "SELECT COUNT(*) AS total_patents FROM patents"

    with sqlite3.connect(db_path) as conn:
        total_patents = int(
            pd.read_sql_query(total_patents_sql, conn)["total_patents"].iloc[0]
        )

    top_inventors = results["q1_top_inventors"].head(3)
    top_companies = results["q2_top_companies"].head(3)
    top_countries = results["q3_top_countries"].head(5)

    top_inventors.to_csv(exports_dir / "top_inventors.csv", index=False)
    top_companies.to_csv(exports_dir / "top_companies.csv", index=False)
    results["q3_top_countries"].to_csv(exports_dir / "country_trends.csv", index=False)

    report_payload = {
        "total_patents": total_patents,
        "top_inventors": _to_records(top_inventors),
        "top_companies": _to_records(top_companies),
        "top_countries": _to_records(top_countries),
    }

    report_json_path = reports_dir / "summary_report.json"
    report_json_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    chart_path = generate_visualizations(results, reports_dir)

    print("\nPatent Analytics Report")
    print("=" * 26)
    print(f"Total patents: {total_patents}")
    print("\nTop 3 inventors:")
    print(top_inventors.to_string(index=False))
    print("\nTop 3 companies:")
    print(top_companies.to_string(index=False))
    print("\nTop countries:")
    print(top_countries.to_string(index=False))
    print(f"\nJSON report: {report_json_path}")
    print(f"Trend chart: {chart_path}")

    return report_payload


if __name__ == "__main__":
    generate_reports()
