"""Generate console, CSV, JSON, and chart reports for patent analytics."""

from __future__ import annotations

from pathlib import Path
import json
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd

from scripts.queries import run_queries
from scripts.logging_config import get_logger, log_pipeline_start, log_pipeline_end, log_stats

logger = get_logger("report")

DEFAULT_DB_PATH = Path("data/patent_analytics.db")
DEFAULT_REPORTS_DIR = Path("outputs/reports")
DEFAULT_EXPORTS_DIR = Path("outputs/exports")


def _to_records(df: pd.DataFrame, limit: int | None = None) -> list[dict]:
    if limit is not None:
        df = df.head(limit)
    return df.to_dict(orient="records")


def generate_visualizations(results: dict[str, pd.DataFrame], reports_dir: Path) -> Path:
    """Create a simple patents-per-year trend chart."""
    logger.info("[Visualizations] Generating trend chart...")
    trend_df = results.get("q4_patents_per_year", pd.DataFrame())
    chart_path = reports_dir / "patents_per_year.png"

    if trend_df.empty:
        logger.warning("[Visualizations] No trend data available, skipping chart")
        return chart_path

    # Ensure year is numeric and handle any indexing issues
    trend_df = trend_df.reset_index(drop=True)
    trend_df["year"] = pd.to_numeric(trend_df["year"], errors="coerce")
    trend_df = trend_df.dropna(subset=["year"])
    
    if trend_df.empty:
        logger.warning("[Visualizations] No valid year data, skipping chart")
        return chart_path

    logger.debug(f"[Visualizations] Plotting {len(trend_df)} years of patent data")
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(trend_df["year"], trend_df["patent_count"], marker="o")
    ax.set_title("Patents per Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Patent Count")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(chart_path)
    plt.close(fig)
    logger.info(f"[Visualizations] Saved chart to {chart_path}")

    return chart_path


def generate_reports(
    db_path: Path = DEFAULT_DB_PATH,
    reports_dir: Path = DEFAULT_REPORTS_DIR,
    exports_dir: Path = DEFAULT_EXPORTS_DIR,
) -> dict:
    """Generate required outputs and return JSON payload."""
    log_pipeline_start("REPORT STAGE")
    
    db_path = Path(db_path)
    reports_dir = Path(reports_dir)
    exports_dir = Path(exports_dir)

    reports_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"[Setup] Created reports directory: {reports_dir}")
    logger.info(f"[Setup] Created exports directory: {exports_dir}")

    logger.info("[Queries] Running analytical queries...")
    results = run_queries(db_path=db_path, exports_dir=exports_dir)
    logger.info(f"[Queries] Completed {len(results)} queries")
    
    total_patents_sql = "SELECT COUNT(*) AS total_patents FROM patents"
    logger.debug("[Statistics] Fetching total patent count...")

    with sqlite3.connect(db_path) as conn:
        total_patents = int(
            pd.read_sql_query(total_patents_sql, conn)["total_patents"].iloc[0]
        )
    logger.info(f"[Statistics] Total patents in database: {total_patents:,}")

    logger.info("[Report Processing] Extracting top entities...")
    top_inventors = results["q1_top_inventors"].head(3)
    logger.info(f"[Report Processing] Top 3 inventors selected")
    
    top_companies = results["q2_top_companies"].head(3)
    logger.info(f"[Report Processing] Top 3 companies selected")
    
    top_countries = results["q3_top_countries"].head(5)
    logger.info(f"[Report Processing] Top 5 countries selected")

    logger.info("[CSV Export] Writing top entities to CSV...")
    top_inventors.to_csv(exports_dir / "top_inventors.csv", index=False)
    logger.debug(f"[CSV Export]   top_inventors.csv")
    
    top_companies.to_csv(exports_dir / "top_companies.csv", index=False)
    logger.debug(f"[CSV Export]   top_companies.csv")
    
    results["q3_top_countries"].to_csv(exports_dir / "country_trends.csv", index=False)
    logger.debug(f"[CSV Export]   country_trends.csv")

    logger.info("[JSON Report] Creating summary report...")
    report_payload = {
        "total_patents": total_patents,
        "top_inventors": _to_records(top_inventors),
        "top_companies": _to_records(top_companies),
        "top_countries": _to_records(top_countries),
    }

    report_json_path = reports_dir / "summary_report.json"
    report_json_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    logger.info(f"[JSON Report] Saved JSON report to {report_json_path}")

    chart_path = generate_visualizations(results, reports_dir)
    
    # Verbose terminal output for humans — more descriptive, multi-line summary
    logger.info("\n=== PATENT ANALYTICS REPORT ===")
    logger.info(f"Database: {db_path}")
    logger.info(f"Total patents: {total_patents:,}")

    # Sizes for key tables (best-effort using query results)
    try:
        counts = {
            "patents": int(results.get("q4_patents_per_year", pd.DataFrame())["patent_count"].sum())
        }
        logger.debug(f"[Diagnostics] Extracted counts: {counts}")
    except Exception as e:
        logger.warning(f"[Diagnostics] Could not extract counts: {e}")
        counts = {}

    logger.info("\n--- Top Inventors (Top 10) ---")
    for line in top_inventors.head(10).to_string(index=False).split("\n"):
        logger.info(line)

    logger.info("\n--- Top Companies (Top 10) ---")
    for line in top_companies.head(10).to_string(index=False).split("\n"):
        logger.info(line)

    logger.info("\n--- Top Countries (Top 10) ---")
    for line in results.get("q3_top_countries", top_countries).head(10).to_string(index=False).split("\n"):
        logger.info(line)

    # Additional diagnostics
    logger.info("\n--- Detailed Statistics ---")
    all_inventors = results.get("q7_inventor_ranking", pd.DataFrame())
    all_companies = results.get("q2_top_companies", pd.DataFrame())
    if not all_inventors.empty:
        total_inventors = int(all_inventors["patent_count"].count())
        top1 = all_inventors.sort_values("patent_count", ascending=False).head(1)
        top1_name = top1["name"].iloc[0]
        top1_count = int(top1["patent_count"].iloc[0])
        logger.info(f"Inventor records: {total_inventors:,} | Top: {top1_name} ({top1_count} patents)")

    if not all_companies.empty:
        total_companies = int(all_companies["patent_count"].count())
        topco = all_companies.sort_values("patent_count", ascending=False).head(1)
        topco_name = topco["name"].iloc[0]
        topco_count = int(topco["patent_count"].iloc[0])
        logger.info(f"Companies with patents: {total_companies:,} | Top: {topco_name} ({topco_count} patents)")

    # Country coverage diagnostic
    country_df = results.get("q3_top_countries", pd.DataFrame())
    if not country_df.empty:
        total_countries = country_df.shape[0]
        known_share = country_df["patent_count"].sum()
        logger.info(f"Countries reported: {total_countries:,} | Patents in stats: {known_share:,}")

    logger.info(f"\nJSON report: {report_json_path}")
    logger.info(f"Trend chart: {chart_path}")
    
    log_stats("REPORT STAGE SUMMARY", {
        "total_patents": total_patents,
        "top_inventors_count": len(top_inventors),
        "top_companies_count": len(top_companies),
        "top_countries_count": len(top_countries),
        "json_report": str(report_json_path),
        "chart_file": str(chart_path),
    })
    log_pipeline_end("REPORT STAGE", "SUCCESS")

    return report_payload


if __name__ == "__main__":
    generate_reports()
