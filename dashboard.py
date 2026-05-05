"""Streamlit dashboard for Global Patent Intelligence Analytics."""

from __future__ import annotations

import json
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
    st.error("Database not found. Run `python main.py` first to build the pipeline.")
    st.stop()


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
    col1.metric("Total Patents", f"{len(patents_df):,}")
    col2.metric("Total Inventors", f"{len(inventors_df):,}")
    col3.metric("Total Companies", f"{len(companies_df):,}")
    linked = len(relationships_df)
    col4.metric("Linked Records", f"{linked:,}")

    st.markdown("---")
    # Year-over-year KPI and two-column overview charts
    yoy_col1, yoy_col2, yoy_col3 = st.columns(3)
    try:
        py = patents_per_year_df.sort_values("year")
        latest_year = int(py["year"].dropna().astype(int).iloc[-1])
        latest_count = int(py[py["year"] == latest_year]["patent_count"].iloc[0])
        prev_count = int(py[py["year"] == (latest_year - 1)]["patent_count"].iloc[0]) if (latest_year - 1) in py["year"].values else None
        delta_pct = None
        if prev_count and prev_count > 0:
            delta_pct = round(((latest_count - prev_count) / prev_count) * 100, 2)
        yoy_col1.metric("Patents (Latest year)", f"{latest_count:,}", f"{delta_pct}% vs prev year" if delta_pct is not None else "")
    except Exception:
        yoy_col1.metric("Patents (Latest year)", "N/A")

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Patent Type Breakdown")
        if not classification_df.empty:
            fig_pie = px.pie(
                classification_df,
                names="label",
                values="count",
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.4,
            )
            fig_pie.update_traces(textinfo="percent+label")
            fig_pie.update_layout(showlegend=False, margin=dict(t=10, b=10))
            _safe_plot(fig_pie)
        else:
            st.info("No classification data.")

    with col_right:
        st.subheader("Patents per Year")
        if not patents_per_year_df.empty:
            fig_year = px.bar(
                patents_per_year_df,
                x="year",
                y="patent_count",
                labels={"year": "Year", "patent_count": "Patents"},
                color_discrete_sequence=["#2563eb"],
            )
            fig_year.update_layout(margin=dict(t=10, b=10))
            _safe_plot(fig_year)
        else:
            st.info("No year data.")

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
        col_a, col_b = st.columns(2)

        with col_a:
            fig_bar = px.bar(
                top_countries_df.head(15),
                x="country",
                y="patent_count",
                color="patent_count",
                color_continuous_scale="Oranges",
                labels={"country": "Country", "patent_count": "Patent Count"},
                title="Top Countries by Patent Count",
            )
            fig_bar.update_layout(coloraxis_showscale=False)
            _safe_plot(fig_bar)

        with col_b:
            fig_map = px.choropleth(
                top_countries_df,
                locations="country",
                locationmode="ISO-3 alpha",
                color="patent_count",
                color_continuous_scale="YlOrRd",
                title="Patent Density by Country",
            )
            fig_map.update_layout(margin=dict(t=40, b=10))
            _safe_plot(fig_map)

        st.subheader("Country Share (Q6 — CTE Query)")
        st.dataframe(
            country_share_df.rename(columns={
                "country": "Country", "patent_count": "Patents", "share_pct": "Share (%)"
            }),
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

    st.markdown(f"**Showing {len(filtered):,} patents**")

    # Display as interactive table with selected columns
    display_cols = ["patent_id", "title", "year", "classification", "filing_date"]
    st.dataframe(
        filtered[display_cols].rename(columns={
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

    if not filtered.empty:
        st.markdown("---")
        st.subheader("Abstract Viewer")
        selected_id = st.selectbox("Select a patent to read its abstract", filtered["patent_id"].tolist())
        row = filtered[filtered["patent_id"] == selected_id].iloc[0]
        st.markdown(f"**{row['title']}**")
        st.markdown(f"*Patent ID: {row['patent_id']} | Filed: {row['filing_date']} | Type: {row['classification']}*")
        st.markdown(row["abstract"] or "_No abstract available._")

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
