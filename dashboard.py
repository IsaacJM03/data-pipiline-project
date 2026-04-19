"""Optional Streamlit dashboard for patent analytics outputs."""

from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import streamlit as st

BASE = Path(__file__).resolve().parent
REPORT_PATH = BASE / "outputs/reports/summary_report.json"
EXPORTS = BASE / "outputs/exports"

st.set_page_config(page_title="Patent Analytics Dashboard", layout="wide")
st.title("Patent Analytics Dashboard")

if not REPORT_PATH.exists():
    st.warning("Run `python main.py` first to generate report files.")
    st.stop()

summary = json.loads(REPORT_PATH.read_text(encoding="utf-8"))

col1, col2, col3 = st.columns(3)
col1.metric("Total patents", summary["total_patents"])
col2.metric("Top inventor", summary["top_inventors"][0]["name"] if summary["top_inventors"] else "N/A")
col3.metric("Top company", summary["top_companies"][0]["name"] if summary["top_companies"] else "N/A")

st.subheader("Top Inventors")
st.dataframe(pd.DataFrame(summary["top_inventors"]))

st.subheader("Top Companies")
st.dataframe(pd.DataFrame(summary["top_companies"]))

country_file = EXPORTS / "country_trends.csv"
if country_file.exists():
    country_df = pd.read_csv(country_file)
    st.subheader("Top Countries")
    st.dataframe(country_df)
