"""Streamlit dashboard for Global Patent Intelligence Analytics."""

from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── paths ──────────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "data/patent_analytics.db"
REPORT_PATH = BASE / "outputs/reports/summary_report.json"
EXPORTS = BASE / "outputs/exports"

# ── page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Global Patent Intelligence",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── guard ──────────────────────────────────────────────────────────────────────
if not DB_PATH.exists():
    with st.spinner("Building database from clean data files — this only runs once..."):
        try:
            import sys
            sys.path.insert(0, str(BASE))
            from scripts.load import run_load
            run_load()
        except Exception as _exc:
            st.error(f"Could not build database: {_exc}\n\nRun `python main.py` locally first.")
            st.stop()
    st.rerun()


# ── data loading ───────────────────────────────────────────────────────────────
@st.cache_data
def load_db(query: str) -> pd.DataFrame:
    """Load a SQL query from the analytics DB with an on-disk cache.

    Cache is keyed by the SHA1 of the query and invalidated when the DB file's
    modification time changes. Parquet is preferred; pickle is a fallback.
    """
    import hashlib

    cache_dir = BASE / "outputs" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    key = hashlib.sha1(query.encode("utf-8")).hexdigest()
    meta_path = cache_dir / f"{key}.meta.json"
    parquet_path = cache_dir / f"{key}.parquet"
    pkl_path = cache_dir / f"{key}.pkl"

    db_mtime = DB_PATH.stat().st_mtime if DB_PATH.exists() else 0

    # Try parquet cache first
    if parquet_path.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("db_mtime") == db_mtime:
                return pd.read_parquet(parquet_path)
        except Exception:
            pass

    # Fallback to pickle cache
    if pkl_path.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("db_mtime") == db_mtime:
                return pd.read_pickle(pkl_path)
        except Exception:
            pass

    # Run the query against SQLite and populate cache
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn)

    # Try to write parquet, otherwise pickle
    meta = {"db_mtime": db_mtime, "query": query}
    try:
        df.to_parquet(parquet_path)
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
    except Exception:
        try:
            df.to_pickle(pkl_path)
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
        except Exception:
            pass

    return df


@st.cache_data
def load_summary() -> dict:
    if REPORT_PATH.exists():
        return json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    return {}


@st.cache_data
def load_csv(name: str) -> pd.DataFrame:
    path = EXPORTS / name
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def top_n_slider(label: str, total_count: int, default_value: int = 10, max_value: int = 20) -> int:
    if total_count <= 1:
        return total_count

    slider_max = min(max_value, total_count)
    slider_default = min(default_value, slider_max)
    return st.slider(label, 1, slider_max, slider_default)


def _safe_plot(fig):
    try:
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.write("(Unable to render chart for this dataset slice)")


COUNTRY_TO_ISO3 = {
    "US": "USA",
    "USA": "USA",
    "UNITED STATES": "USA",
    "UNITED STATES OF AMERICA": "USA",
    "JP": "JPN",
    "JPN": "JPN",
    "JPX": "JPN",
    "JAPAN": "JPN",
    "DE": "DEU",
    "DEU": "DEU",
    "DEX": "DEU",
    "GERMANY": "DEU",
    "FR": "FRA",
    "FRA": "FRA",
    "FRANCE": "FRA",
    "GB": "GBR",
    "UK": "GBR",
    "GBR": "GBR",
    "UNITED KINGDOM": "GBR",
    "DK": "DNK",
    "DNK": "DNK",
    "DKX": "DNK",
    "DENMARK": "DNK",
    "KR": "KOR",
    "KOR": "KOR",
    "KOREA": "KOR",
    "SOUTH KOREA": "KOR",
    "CN": "CHN",
    "CHN": "CHN",
    "CHINA": "CHN",
    "TW": "TWN",
    "TWN": "TWN",
    "TAIWAN": "TWN",
    "CA": "CAN",
    "CAN": "CAN",
    "CANADA": "CAN",
    "IL": "ISR",
    "ISR": "ISR",
    "ISRAEL": "ISR",
    "BR": "BRA",
    "BRA": "BRA",
    "BRAZIL": "BRA",
    "IN": "IND",
    "IND": "IND",
    "INDIA": "IND",
    "AU": "AUS",
    "AUS": "AUS",
    "AUSTRALIA": "AUS",
    "NL": "NLD",
    "NLD": "NLD",
    "NETHERLANDS": "NLD",
    "CH": "CHE",
    "CHE": "CHE",
    "SWITZERLAND": "CHE",
    "SE": "SWE",
    "SWE": "SWE",
    "SWEDEN": "SWE",
    "NO": "NOR",
    "NOR": "NOR",
    "NORWAY": "NOR",
    "ES": "ESP",
    "ESP": "ESP",
    "SPAIN": "ESP",
    "IT": "ITA",
    "ITA": "ITA",
    "ITALY": "ITA",
    "MX": "MEX",
    "MEX": "MEX",
    "MEXICO": "MEX",
    "RU": "RUS",
    "RUS": "RUS",
    "RUSSIA": "RUS",
    "SG": "SGP",
    "SGP": "SGP",
    "SINGAPORE": "SGP",
}


def country_to_iso3(country: str | None) -> str | None:
    if country is None or pd.isna(country):
        return None

    value = str(country).strip().upper()
    if not value:
        return None

    if value in COUNTRY_TO_ISO3:
        return COUNTRY_TO_ISO3[value]

    if len(value) == 3 and value.isalpha():
        return value

    return None


def build_country_geo_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "country" not in frame.columns:
        return pd.DataFrame(columns=["country", "patent_count", "iso3"])

    geo = frame.copy()
    geo["iso3"] = geo["country"].apply(country_to_iso3)
    geo["country_label"] = geo["country"].fillna("Unknown")
    return geo


def add_cumulative_share(frame: pd.DataFrame, value_col: str, label_col: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    ordered = frame.copy()
    ordered[value_col] = pd.to_numeric(ordered[value_col], errors="coerce").fillna(0)
    ordered = ordered.sort_values(value_col, ascending=False).reset_index(drop=True)
    total = ordered[value_col].sum()
    if total <= 0:
        ordered["share_pct"] = 0.0
        ordered["cum_share_pct"] = 0.0
        return ordered

    ordered["share_pct"] = (ordered[value_col] / total * 100).round(2)
    ordered["cum_share_pct"] = ordered["share_pct"].cumsum().round(2)
    ordered["rank"] = ordered.index + 1
    return ordered


def summarize_concentration(frame: pd.DataFrame, value_col: str) -> dict[str, float]:
    if frame.empty or value_col not in frame.columns:
        return {"total": 0.0, "top1_pct": 0.0, "top3_pct": 0.0, "top5_pct": 0.0, "hhi": 0.0, "entropy": 0.0}

    values = pd.to_numeric(frame[value_col], errors="coerce").fillna(0).sort_values(ascending=False)
    total = float(values.sum())
    if total <= 0:
        return {"total": 0.0, "top1_pct": 0.0, "top3_pct": 0.0, "top5_pct": 0.0, "hhi": 0.0, "entropy": 0.0}

    shares = values / total
    top1_pct = float(shares.head(1).sum() * 100)
    top3_pct = float(shares.head(3).sum() * 100)
    top5_pct = float(shares.head(5).sum() * 100)
    hhi = float((shares ** 2).sum())
    entropy = float(-(shares * shares.map(lambda x: math.log2(x) if x > 0 else 0)).sum())
    return {
        "total": total,
        "top1_pct": round(top1_pct, 2),
        "top3_pct": round(top3_pct, 2),
        "top5_pct": round(top5_pct, 2),
        "hhi": round(hhi, 4),
        "entropy": round(entropy, 3),
    }


# ── queries ────────────────────────────────────────────────────────────────────
patents_df = load_db("SELECT * FROM patents")
inventors_df = load_db("SELECT * FROM inventors")
companies_df = load_db("SELECT * FROM companies")
relationships_df = load_db("SELECT * FROM relationships")

top_inventors_df = load_db("""
    SELECT i.inventor_id, i.name, i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN inventors i ON i.inventor_id = r.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
    ORDER BY patent_count DESC
    LIMIT 20
""")

top_companies_df = load_db("""
    SELECT c.company_id, c.name, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN companies c ON c.company_id = r.company_id
    GROUP BY c.company_id, c.name
    ORDER BY patent_count DESC
    LIMIT 20
""")

top_countries_df = load_db("""
    SELECT i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN inventors i ON i.inventor_id = r.inventor_id
    WHERE i.country IS NOT NULL AND i.country != ''
    GROUP BY i.country
    ORDER BY patent_count DESC
    LIMIT 20
""")

patents_per_year_df = load_db("""
    SELECT year, COUNT(*) AS patent_count FROM patents
    GROUP BY year ORDER BY year
""")

country_share_df = load_db("""
    WITH country_counts AS (
        SELECT i.country, COUNT(DISTINCT r.patent_id) AS patent_count
        FROM relationships r JOIN inventors i ON i.inventor_id = r.inventor_id
        WHERE i.country IS NOT NULL AND i.country != ''
        GROUP BY i.country
    ),
    total AS (SELECT COUNT(DISTINCT patent_id) AS total FROM relationships)
    SELECT c.country, c.patent_count,
           ROUND((1.0 * c.patent_count / t.total) * 100, 2) AS share_pct
    FROM country_counts c CROSS JOIN total t
    ORDER BY c.patent_count DESC
""")

country_geo_df = build_country_geo_frame(country_share_df)
country_geo_map_df = country_geo_df[country_geo_df["iso3"].notna()].copy()
country_geo_map_df = country_geo_map_df[country_geo_map_df["iso3"].str.len() == 3]
country_concentration = summarize_concentration(country_share_df, "patent_count")
country_share_rank_df = add_cumulative_share(country_share_df, "patent_count", "country")

inventor_ranking_df = load_db("""
    SELECT inventor_id, name, country, patent_count,
           DENSE_RANK() OVER (ORDER BY patent_count DESC, name ASC) AS inventor_rank
    FROM (
        SELECT i.inventor_id, i.name, i.country, COUNT(DISTINCT r.patent_id) AS patent_count
        FROM relationships r JOIN inventors i ON i.inventor_id = r.inventor_id
        GROUP BY i.inventor_id, i.name, i.country
    ) sub
    ORDER BY inventor_rank ASC
""")

classification_df = load_db("""
    SELECT classification,
           CASE classification WHEN 'B1' THEN 'Granted (no prior publication)'
                               WHEN 'B2' THEN 'Granted (prior publication)' END AS label,
           COUNT(*) AS count
    FROM patents
    WHERE classification IS NOT NULL
    GROUP BY classification
    ORDER BY count DESC
""")

join_df = load_db("""
    SELECT p.patent_id, p.title, p.year, p.classification,
           i.name AS inventor_name, i.country,
           c.name AS company_name
    FROM relationships r
    JOIN patents p ON p.patent_id = r.patent_id
    JOIN inventors i ON i.inventor_id = r.inventor_id
    JOIN companies c ON c.company_id = r.company_id
    ORDER BY p.year DESC, p.patent_id
""")

summary = load_summary()

# ── sidebar navigation ─────────────────────────────────────────────────────────
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/9/9d/USPTO_seal.svg/240px-USPTO_seal.svg.png", width=80)
st.sidebar.title("Patent Analytics")
st.sidebar.markdown("Global Patent Intelligence Dashboard")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Top Inventors", "Top Companies", "Countries", "Trends", "Patent Explorer", "SQL Queries"],
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Database: `data/patent_analytics.db`")
st.sidebar.caption(f"Source: PatentsView (USPTO)")

# ══════════════════════════════════════════════════════════════════════════════
# OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "Overview":
    st.title("🔬 Global Patent Intelligence Dashboard")
    st.markdown("An end-to-end analytics pipeline built on **PatentsView** granted patent data from the USPTO.")

    # KPI metrics
    col1, col2, col3, col4 = st.columns(4)
    total_patents = len(patents_df)
    total_inventors = len(inventors_df)
    total_companies = len(companies_df)
    linked = len(relationships_df)
    col1.metric("Total Patents", f"{total_patents:,}")
    col2.metric("Total Inventors", f"{total_inventors:,}")
    col3.metric("Total Companies", f"{total_companies:,}")
    col4.metric("Linked Records", f"{linked:,}")

    kpi2a, kpi2b, kpi2c = st.columns(3)
    latest_year = patents_per_year_df["year"].dropna().astype(int).max() if not patents_per_year_df.empty else None
    latest_year_count = int(patents_per_year_df.loc[patents_per_year_df["year"] == latest_year, "patent_count"].iloc[0]) if latest_year is not None and not patents_per_year_df.empty else 0
    linked_ratio = round((linked / total_patents) * 100, 2) if total_patents else 0
    country_coverage = len(country_geo_map_df)
    kpi2a.metric("Latest Year Patents", f"{latest_year_count:,}", f"{latest_year}" if latest_year else "")
    kpi2b.metric("Linked Coverage", f"{linked_ratio}%")
    kpi2c.metric("Mapped Countries", f"{country_coverage:,}")

    st.markdown("---")
    trend_col, composition_col = st.columns(2)

    with trend_col:
        st.subheader("Patent Growth and Momentum")
        if not patents_per_year_df.empty:
            py = patents_per_year_df.sort_values("year").copy()
            py["rolling_3yr"] = py["patent_count"].rolling(window=3, min_periods=1).mean()
            growth_fig = go.Figure()
            growth_fig.add_trace(go.Scatter(
                x=py["year"],
                y=py["patent_count"],
                mode="lines+markers",
                line=dict(color="#2563eb", width=2),
                name="Annual patents",
            ))
            growth_fig.add_trace(go.Scatter(
                x=py["year"],
                y=py["rolling_3yr"],
                mode="lines",
                line=dict(color="#0f766e", width=3, dash="dash"),
                name="3-year rolling mean",
            ))
            growth_fig.update_layout(
                title="Annual Patent Production",
                xaxis_title="Year",
                yaxis_title="Patent Count",
                hovermode="x unified",
                margin=dict(t=40, b=10),
            )
            _safe_plot(growth_fig)

            growth_delta = None
            if len(py) >= 2:
                current = float(py["patent_count"].iloc[-1])
                previous = float(py["patent_count"].iloc[-2])
                if previous > 0:
                    growth_delta = round(((current - previous) / previous) * 100, 2)
            st.caption(f"Latest year: {latest_year} | Year-over-year change: {growth_delta}%" if growth_delta is not None else f"Latest year: {latest_year}")
        else:
            st.info("No year data.")

    with composition_col:
        st.subheader("Classification Mix and Coverage")
        if not classification_df.empty:
            class_fig = px.area(
                classification_df,
                x="classification",
                y="count",
                color="classification",
                line_group="classification",
                title="Classification Distribution",
            )
            class_fig.update_layout(
                showlegend=False,
                margin=dict(t=40, b=10),
                xaxis_title="Classification",
                yaxis_title="Patents",
            )
            _safe_plot(class_fig)
        else:
            st.info("No classification data.")

        concentration = summarize_concentration(top_companies_df, "patent_count")
        c1, c2 = st.columns(2)
        c1.metric("Top 1 Share", f"{concentration['top1_pct']:.2f}%")
        c2.metric("Top 5 Share", f"{concentration['top5_pct']:.2f}%")

    st.markdown("---")
    st.subheader("Concentration Profile")
    if not top_companies_df.empty and not top_inventors_df.empty:
        pareto_left, pareto_right = st.columns(2)

        with pareto_left:
            company_pareto = add_cumulative_share(top_companies_df.copy(), "patent_count", "name")
            pareto_fig = go.Figure()
            pareto_fig.add_trace(go.Bar(
                x=company_pareto["name"],
                y=company_pareto["patent_count"],
                marker_color="#059669",
                name="Companies",
            ))
            pareto_fig.add_trace(go.Scatter(
                x=company_pareto["name"],
                y=company_pareto["cum_share_pct"],
                mode="lines+markers",
                yaxis="y2",
                line=dict(color="#0f172a", width=2),
                name="Cumulative share (%)",
            ))
            pareto_fig.update_layout(
                title="Top Companies Pareto Curve",
                yaxis=dict(title="Patent Count"),
                yaxis2=dict(title="Cumulative Share (%)", overlaying="y", side="right", range=[0, 100]),
                xaxis_tickangle=-35,
                margin=dict(t=40, b=80),
                legend=dict(orientation="h"),
            )
            _safe_plot(pareto_fig)

        with pareto_right:
            inventor_pareto = add_cumulative_share(top_inventors_df.copy(), "patent_count", "name")
            inv_fig = go.Figure()
            inv_fig.add_trace(go.Bar(
                x=inventor_pareto["name"],
                y=inventor_pareto["patent_count"],
                marker_color="#2563eb",
                name="Inventors",
            ))
            inv_fig.add_trace(go.Scatter(
                x=inventor_pareto["name"],
                y=inventor_pareto["cum_share_pct"],
                mode="lines+markers",
                yaxis="y2",
                line=dict(color="#7c3aed", width=2),
                name="Cumulative share (%)",
            ))
            inv_fig.update_layout(
                title="Top Inventors Pareto Curve",
                yaxis=dict(title="Patent Count"),
                yaxis2=dict(title="Cumulative Share (%)", overlaying="y", side="right", range=[0, 100]),
                xaxis_tickangle=-35,
                margin=dict(t=40, b=80),
                legend=dict(orientation="h"),
            )
            _safe_plot(inv_fig)

    st.markdown("---")
    st.subheader("Top Companies Snapshot")
    if not top_companies_df.empty:
        try:
            treemap = px.treemap(
                top_companies_df.head(50),
                path=[px.Constant("Companies"), "name"],
                values="patent_count",
                title="Top Companies (by patent count)",
            )
            _safe_plot(treemap)
        except Exception:
            st.info("Treemap cannot be rendered for this dataset slice.")
    else:
        st.info("No company patent stats available yet.")

    st.markdown("---")
    st.subheader("Pipeline Architecture")
    st.markdown("""
    ```
    PatentsView (USPTO)  →  extract.py / streaming_pipeline.py
         ↓
    pandas transform     →  clean_patents / inventors / companies / relationships CSVs
         ↓
    SQLite (load.py)     →  data/patent_analytics.db
         ↓
    SQL queries          →  sql/queries.sql  (Q1 – Q7)
         ↓
    Reports              →  outputs/reports/  +  outputs/exports/
         ↓
    Dashboard            →  dashboard.py  (this app)
    ```
    """)

# ══════════════════════════════════════════════════════════════════════════════
# TOP INVENTORS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Top Inventors":
    st.title("👩‍🔬 Top Inventors")
    st.markdown("Ranked by number of distinct patents filed (Q1 — Top Inventors query).")

    if top_inventors_df.empty:
        st.warning("No inventor–patent links found in the relationships table.")
    else:
        n = top_n_slider("Show top N inventors", len(top_inventors_df))
        df_top = top_inventors_df.head(n)
        inventor_stats = summarize_concentration(df_top, "patent_count")

        stat_cols = st.columns(4)
        stat_cols[0].metric("Top 1 Share", f"{inventor_stats['top1_pct']:.2f}%")
        stat_cols[1].metric("Top 3 Share", f"{inventor_stats['top3_pct']:.2f}%")
        stat_cols[2].metric("Top 5 Share", f"{inventor_stats['top5_pct']:.2f}%")
        stat_cols[3].metric("HHI", f"{inventor_stats['hhi']:.4f}")

        fig = px.bar(
            df_top,
            x="patent_count",
            y="name",
            orientation="h",
            color="patent_count",
            color_continuous_scale="Blues",
            labels={"patent_count": "Patent Count", "name": "Inventor"},
            title=f"Top {n} Inventors by Patent Count",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
        _safe_plot(fig)

        inventor_pareto = add_cumulative_share(df_top.copy(), "patent_count", "name")
        pareto_fig = go.Figure()
        pareto_fig.add_trace(go.Bar(x=inventor_pareto["name"], y=inventor_pareto["patent_count"], marker_color="#2563eb", name="Patent Count"))
        pareto_fig.add_trace(go.Scatter(x=inventor_pareto["name"], y=inventor_pareto["cum_share_pct"], mode="lines+markers", yaxis="y2", line=dict(color="#7c3aed", width=2), name="Cumulative share (%)"))
        pareto_fig.update_layout(
            title="Inventor Concentration Curve",
            yaxis=dict(title="Patent Count"),
            yaxis2=dict(title="Cumulative Share (%)", overlaying="y", side="right", range=[0, 100]),
            xaxis_tickangle=-35,
            margin=dict(t=40, b=80),
            legend=dict(orientation="h"),
        )
        _safe_plot(pareto_fig)

        st.subheader("Inventor Ranking (Q7 — Window Function)")
        st.dataframe(
            inventor_ranking_df[["inventor_rank", "name", "country", "patent_count"]],
            width='stretch',
            hide_index=True,
        )

    st.subheader("All Inventors in Database")
    search = st.text_input("Search inventor name")
    inv_display = inventors_df.copy()
    if search:
        inv_display = inv_display[inv_display["name"].str.contains(search, case=False, na=False)]
    st.dataframe(inv_display[["name", "country"]].rename(columns={"name": "Name", "country": "Country"}),
                 width='stretch', hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# TOP COMPANIES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Top Companies":
    st.title("🏢 Top Companies (Assignees)")
    st.markdown("Companies ranked by number of patents owned (Q2 — Top Companies query).")

    if top_companies_df.empty:
        st.warning("No company–patent links found in the relationships table.")
    else:
        n = top_n_slider("Show top N companies", len(top_companies_df))
        df_top = top_companies_df.head(n)
        company_stats = summarize_concentration(df_top, "patent_count")

        stat_cols = st.columns(4)
        stat_cols[0].metric("Top 1 Share", f"{company_stats['top1_pct']:.2f}%")
        stat_cols[1].metric("Top 3 Share", f"{company_stats['top3_pct']:.2f}%")
        stat_cols[2].metric("Top 5 Share", f"{company_stats['top5_pct']:.2f}%")
        stat_cols[3].metric("HHI", f"{company_stats['hhi']:.4f}")

        fig = px.bar(
            df_top,
            x="patent_count",
            y="name",
            orientation="h",
            color="patent_count",
            color_continuous_scale="Greens",
            labels={"patent_count": "Patent Count", "name": "Company"},
            title=f"Top {n} Companies by Patent Count",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
        _safe_plot(fig)

        company_pareto = add_cumulative_share(df_top.copy(), "patent_count", "name")
        pareto_fig = go.Figure()
        pareto_fig.add_trace(go.Bar(x=company_pareto["name"], y=company_pareto["patent_count"], marker_color="#059669", name="Patent Count"))
        pareto_fig.add_trace(go.Scatter(x=company_pareto["name"], y=company_pareto["cum_share_pct"], mode="lines+markers", yaxis="y2", line=dict(color="#0f172a", width=2), name="Cumulative share (%)"))
        pareto_fig.update_layout(
            title="Company Concentration Curve",
            yaxis=dict(title="Patent Count"),
            yaxis2=dict(title="Cumulative Share (%)", overlaying="y", side="right", range=[0, 100]),
            xaxis_tickangle=-35,
            margin=dict(t=40, b=80),
            legend=dict(orientation="h"),
        )
        _safe_plot(pareto_fig)

        st.subheader("Data Table")
        st.dataframe(
            df_top[["name", "patent_count"]].rename(columns={"name": "Company", "patent_count": "Patents"}),
            width='stretch',
            hide_index=True,
        )

    st.markdown(f"**Total companies in database:** {len(companies_df):,}")

# ══════════════════════════════════════════════════════════════════════════════
# COUNTRIES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Countries":
    st.title("🌍 Countries")
    st.markdown("Countries producing the most patents, based on inventor location (Q3 & Q6).")

    if top_countries_df.empty:
        st.info("Country data is limited — most inventor records lack a country code in this dataset slice.")
    else:
        stats = summarize_concentration(country_share_rank_df, "patent_count")
        total_countries = len(country_share_rank_df)
        mapped_countries = len(country_geo_map_df)

        metric_cols = st.columns(4)
        metric_cols[0].metric("Mapped Countries", f"{mapped_countries:,}")
        metric_cols[1].metric("Country Categories", f"{total_countries:,}")
        metric_cols[2].metric("Top Country Share", f"{stats['top1_pct']:.2f}%")
        metric_cols[3].metric("Country Entropy", f"{stats['entropy']:.3f}")

        col_a, col_b = st.columns(2)

        with col_a:
            country_bar_df = country_share_rank_df.head(15)
            fig_bar = px.bar(
                country_bar_df,
                x="patent_count",
                y="country",
                orientation="h",
                color="share_pct",
                color_continuous_scale="Oranges",
                labels={"country": "Country", "patent_count": "Patent Count", "share_pct": "Share (%)"},
                title="Top Countries by Patent Count",
            )
            fig_bar.update_layout(coloraxis_showscale=True, yaxis=dict(categoryorder="total ascending"))
            _safe_plot(fig_bar)

        with col_b:
            if not country_geo_map_df.empty:
                fig_map = px.choropleth(
                    country_geo_map_df,
                    locations="iso3",
                    locationmode="ISO-3",
                    color="patent_count",
                    hover_name="country_label",
                    hover_data={"iso3": True, "patent_count": True, "share_pct": ":.2f"},
                    color_continuous_scale="YlOrRd",
                    title="Patent Density by Country",
                )
                fig_map.update_geos(showframe=False, showcoastlines=True, projection_type="natural earth")
                fig_map.update_layout(margin=dict(t=40, b=10))
                _safe_plot(fig_map)
            else:
                st.info("No ISO-3 countries were available for the geo map.")

        st.subheader("Country Share and Concentration")
        share_left, share_right = st.columns(2)

        with share_left:
            pareto_country = add_cumulative_share(country_share_rank_df.copy(), "patent_count", "country")
            pareto_fig = go.Figure()
            pareto_fig.add_trace(go.Bar(
                x=pareto_country["country"],
                y=pareto_country["patent_count"],
                marker_color="#f97316",
                name="Patent Count",
            ))
            pareto_fig.add_trace(go.Scatter(
                x=pareto_country["country"],
                y=pareto_country["cum_share_pct"],
                mode="lines+markers",
                yaxis="y2",
                line=dict(color="#1f2937", width=2),
                name="Cumulative share (%)",
            ))
            pareto_fig.update_layout(
                title="Country Concentration Curve",
                yaxis=dict(title="Patent Count"),
                yaxis2=dict(title="Cumulative Share (%)", overlaying="y", side="right", range=[0, 100]),
                xaxis_tickangle=-35,
                margin=dict(t=40, b=80),
                legend=dict(orientation="h"),
            )
            _safe_plot(pareto_fig)

        with share_right:
            if not country_share_rank_df.empty:
                fig_hist = px.histogram(
                    country_share_rank_df,
                    x="patent_count",
                    nbins=min(20, max(5, len(country_share_rank_df) // 2)),
                    title="Distribution of Patent Counts per Country",
                    labels={"patent_count": "Patent Count"},
                    color_discrete_sequence=["#0ea5e9"],
                )
                fig_hist.update_layout(margin=dict(t=40, b=10))
                _safe_plot(fig_hist)

        st.dataframe(
            country_share_rank_df.rename(columns={
                "country": "Country",
                "patent_count": "Patents",
                "share_pct": "Share (%)",
                "cum_share_pct": "Cum. Share (%)",
            })[["Country", "Patents", "Share (%)", "Cum. Share (%)", "rank"]],
            width='stretch',
            hide_index=True,
        )

    with st.expander("Country distribution across all inventors"):
        country_counts = inventors_df["country"].value_counts(dropna=False).reset_index()
        country_counts.columns = ["country", "inventor_count"]
        st.dataframe(country_counts.head(30), width='stretch', hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# TRENDS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Trends":
    st.title("📈 Patent Trends Over Time")
    st.markdown("How the number of patents has changed year by year (Q4 — Trends query).")

    if patents_per_year_df.empty:
        st.warning("No year data available.")
    else:
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=patents_per_year_df["year"],
            y=patents_per_year_df["patent_count"],
            mode="lines+markers",
            fill="tozeroy",
            line=dict(color="#2563eb", width=2),
            marker=dict(size=8),
            name="Patents",
        ))
        fig_line.update_layout(
            title="Patents per Year",
            xaxis_title="Year",
            yaxis_title="Number of Patents",
            hovermode="x unified",
        )
        _safe_plot(fig_line)

        st.subheader("Data Table")
        st.dataframe(
            patents_per_year_df.rename(columns={"year": "Year", "patent_count": "Patents"}),
            width='stretch',
            hide_index=True,
        )

    st.markdown("---")
    st.subheader("Patent Type Trend")
    classification_year_df = load_db("""
        SELECT year, classification, COUNT(*) AS count
        FROM patents
        WHERE classification IS NOT NULL
        GROUP BY year, classification
        ORDER BY year, classification
    """)
    if not classification_year_df.empty:
        fig_class = px.bar(
            classification_year_df,
            x="year",
            y="count",
            color="classification",
            barmode="stack",
            labels={"year": "Year", "count": "Patents", "classification": "Type"},
            color_discrete_map={"B1": "#22c55e", "B2": "#3b82f6"},
            title="Patent Type Distribution by Year",
        )
        _safe_plot(fig_class)

# ══════════════════════════════════════════════════════════════════════════════
# PATENT EXPLORER
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Patent Explorer":
    st.title("🔍 Patent Explorer")
    st.markdown("Browse and search the full patent dataset.")

    col_search, col_filter = st.columns([3, 1])
    with col_search:
        keyword = st.text_input("Search patents by title or abstract", placeholder="e.g. machine learning")
    with col_filter:
        years = sorted(patents_df["year"].dropna().unique().tolist())
        year_filter = st.multiselect("Filter by year", years, default=years)

    filtered = patents_df.copy()
    if keyword:
        mask = (
            filtered["title"].str.contains(keyword, case=False, na=False) |
            filtered["abstract"].str.contains(keyword, case=False, na=False)
        )
        filtered = filtered[mask]
    if year_filter:
        filtered = filtered[filtered["year"].isin(year_filter)]

    max_patent_window = min(1_000_000, len(filtered))
    if max_patent_window >= 500_000:
        default_window = min(750_000, max_patent_window)
        patent_window = st.slider("Patent preview window", 500_000, max_patent_window, default_window, step=50_000)
    else:
        patent_window = st.slider("Patent preview window", 1, max_patent_window, max_patent_window)

    page_size = st.selectbox("Rows per page", [50, 100, 250], index=1)
    window_df = filtered.head(patent_window).copy()
    total_pages = max(1, math.ceil(len(window_df) / page_size))
    page_number = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
    start_idx = (int(page_number) - 1) * page_size
    end_idx = start_idx + page_size
    page_df = window_df.iloc[start_idx:end_idx].copy()

    st.markdown(f"**Matching patents:** {len(filtered):,} | **Windowed preview:** {len(window_df):,} | **Page:** {int(page_number)} / {total_pages}")

    explorer_cols = st.columns(4)
    explorer_cols[0].metric("Window Size", f"{patent_window:,}")
    explorer_cols[1].metric("Page Size", f"{page_size:,}")
    explorer_cols[2].metric("Displayed Rows", f"{len(page_df):,}")
    explorer_cols[3].metric("Unique Years", f"{window_df['year'].nunique():,}")

    display_cols = ["patent_id", "title", "year", "classification", "filing_date"]
    st.dataframe(
        page_df[display_cols].rename(columns={
            "patent_id": "Patent ID",
            "title": "Title",
            "year": "Year",
            "classification": "Type",
            "filing_date": "Filing Date",
        }),
        width='stretch',
        hide_index=True,
        height=400,
    )

    if not page_df.empty:
        st.markdown("---")
        st.subheader("Abstract Viewer")
        selected_id = st.selectbox("Select a patent to read its abstract", page_df["patent_id"].tolist())
        row = page_df[page_df["patent_id"] == selected_id].iloc[0]
        st.markdown(f"**{row['title']}**")
        st.markdown(f"*Patent ID: {row['patent_id']} | Filed: {row['filing_date']} | Type: {row['classification']}*")
        st.markdown(row["abstract"] or "_No abstract available._")

        st.markdown("---")
        st.subheader("Slice Diagnostics")
        slice_metrics = st.columns(4)
        slice_metrics[0].metric("Earliest Year", f"{int(window_df['year'].min())}" if window_df["year"].notna().any() else "N/A")
        slice_metrics[1].metric("Latest Year", f"{int(window_df['year'].max())}" if window_df["year"].notna().any() else "N/A")
        slice_metrics[2].metric("Classification Types", f"{window_df['classification'].nunique():,}")
        slice_metrics[3].metric("Abstract Coverage", f"{round(window_df['abstract'].notna().mean() * 100, 2)}%")

        slice_years = window_df["year"].value_counts(dropna=False).sort_index().reset_index()
        slice_years.columns = ["year", "patent_count"]
        if not slice_years.empty:
            fig_slice = px.line(slice_years, x="year", y="patent_count", markers=True, title="Year Distribution in Window")
            _safe_plot(fig_slice)

    st.markdown("---")
    st.subheader("Q5 — JOIN Query: Patents with Inventors and Companies")
    if join_df.empty:
        st.info("The JOIN result is empty because the relationships table has very few linked records in this dataset slice.")
    else:
        st.dataframe(join_df, width='stretch', hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# SQL QUERIES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "SQL Queries":
    st.title("🗄️ SQL Analytical Queries")
    st.markdown("Results for all seven required analytical queries (Q1–Q7).")

    query_labels = {
        "Q1 — Top Inventors": top_inventors_df,
        "Q2 — Top Companies": top_companies_df,
        "Q3 — Top Countries": top_countries_df,
        "Q4 — Patents per Year": patents_per_year_df,
        "Q5 — JOIN (Patents × Inventors × Companies)": join_df,
        "Q6 — Country Share (CTE)": country_share_df,
        "Q7 — Inventor Ranking (Window Function)": inventor_ranking_df,
    }

    for label, df in query_labels.items():
        with st.expander(label, expanded=label.startswith("Q4")):
            if df.empty:
                st.info("No results — relationships table has limited linked records in this dataset.")
            else:
                st.dataframe(df, width='stretch', hide_index=True)
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    f"⬇ Download {label.split('—')[0].strip()}.csv",
                    data=csv,
                    file_name=f"{label.split('—')[0].strip().lower().replace(' ', '_')}.csv",
                    mime="text/csv",
                )

    st.markdown("---")
    st.subheader("View SQL Source")
    sql_path = BASE / "sql/queries.sql"
    if sql_path.exists():
        with st.expander("sql/queries.sql"):
            st.code(sql_path.read_text(encoding="utf-8"), language="sql")

    schema_path = BASE / "sql/schema.sql"
    if schema_path.exists():
        with st.expander("sql/schema.sql"):
            st.code(schema_path.read_text(encoding="utf-8"), language="sql")
