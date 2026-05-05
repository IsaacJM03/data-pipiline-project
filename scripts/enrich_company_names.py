"""
Enrich company names in the patent analytics database.

Downloads g_assignee_disambiguated.tsv from PatentsView, maps
disambig_assignee_id → disambig_assignee_organization, and updates
the companies table and clean_companies.csv in-place.

Usage:
    python scripts/enrich_company_names.py
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path

import pandas as pd

# ── paths ──────────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
RAW_DIR = BASE / "data/raw"
DB_PATH = BASE / "data/patent_analytics.db"
CLEAN_COMPANIES_CSV = BASE / "data/processed/clean_companies.csv"

ASSIGNEE_ZIP_URL = (
    "https://s3.amazonaws.com/data.patentsview.org/download/g_assignee_disambiguated.tsv.zip"
)
PERSISTENT_ASSIGNEE_ZIP_URL = (
    "https://s3.amazonaws.com/data.patentsview.org/download/g_persistent_assignee.tsv.zip"
)
ASSIGNEE_ZIP = RAW_DIR / "g_assignee_disambiguated.tsv.zip"
ASSIGNEE_TSV = RAW_DIR / "g_assignee_disambiguated.tsv"
PERSISTENT_ASSIGNEE_ZIP = RAW_DIR / "g_persistent_assignee.tsv.zip"
PERSISTENT_ASSIGNEE_TSV = RAW_DIR / "g_persistent_assignee.tsv"

S3_BASE = "https://s3.amazonaws.com/data.patentsview.org/download"
ALL_TSV_FILES = [
    "g_patent.tsv",
    "g_patent_abstract.tsv",
    "g_inventor_disambiguated.tsv",
    "g_inventor_not_disambiguated.tsv",
    "g_location_disambiguated.tsv",
    "g_location_not_disambiguated.tsv",
    "g_persistent_assignee.tsv",
    "g_persistent_inventor.tsv",
    "g_assignee_disambiguated.tsv",
    "g_cpc_title.tsv",
    "g_ipc_at_issue.tsv",
    "g_uspc_at_issue.tsv",
    "g_us_application_citation.tsv",
    "g_us_rel_doc.tsv",
    "g_us_term_of_grant.tsv",
    "g_gov_interest.tsv",
    "g_gov_interest_org.tsv",
    "g_gov_interest_contracts.tsv",
    "g_other_reference.tsv",
    "g_pct_data.tsv",
    "g_rel_app_text.tsv",
    "g_botanic.tsv",
]


# ── helpers ────────────────────────────────────────────────────────────────────
def download(url: str, dest: Path) -> None:
    print(f"  Downloading {url}")
    print(f"  → {dest}  (this may take a minute…)")
    tool = shutil.which("curl") or shutil.which("wget")
    if not tool:
        raise RuntimeError("Neither curl nor wget found on PATH.")
    if "curl" in tool:
        subprocess.run(
            ["curl", "-fL", "--retry", "3", "--progress-bar", "-o", str(dest), url],
            check=True,
        )
    else:
        subprocess.run(["wget", "-q", "--show-progress", "-O", str(dest), url], check=True)


def extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """Try multiple extractors; macOS ditto handles edge-case zips."""
    print(f"  Extracting {zip_path.name}…")

    # 1. Standard zipfile
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dest_dir)
        print("  Extracted via zipfile.")
        return
    except zipfile.BadZipFile:
        pass

    # 2. macOS ditto
    if shutil.which("ditto"):
        result = subprocess.run(
            ["ditto", "-xk", str(zip_path), str(dest_dir)],
            capture_output=True,
        )
        if result.returncode == 0:
            print("  Extracted via ditto.")
            return

    # 3. system unzip
    if shutil.which("unzip"):
        result = subprocess.run(
            ["unzip", "-o", str(zip_path), "-d", str(dest_dir)],
            capture_output=True,
        )
        if result.returncode == 0:
            print("  Extracted via unzip.")
            return

    raise RuntimeError(
        f"Could not extract {zip_path}. "
        "Try manually extracting it to data/raw/g_assignee_disambiguated.tsv"
    )


def remove_zip(zip_path: Path) -> None:
    if zip_path.exists():
        zip_path.unlink()


def download_all_zips() -> None:
    """Download all known PatentsView TSV zip files to data/raw if missing."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print("\n[Download] Ensuring all known TSV ZIP files exist in data/raw…")

    for tsv_name in ALL_TSV_FILES:
        zip_name = f"{tsv_name}.zip"
        zip_path = RAW_DIR / zip_name
        tsv_path = RAW_DIR / tsv_name

        if zip_path.exists():
            print(f"  SKIP zip exists: {zip_name}")
            continue

        url = f"{S3_BASE}/{zip_name}"
        try:
            download(url, zip_path)
        except Exception as exc:
            print(f"  WARNING: Could not download {zip_name}: {exc}")
            continue

        if not tsv_path.exists():
            try:
                extract_zip(zip_path, RAW_DIR)
                remove_zip(zip_path)
            except Exception as exc:
                print(f"  WARNING: Could not extract {zip_name}: {exc}")


def ensure_tsv_from_own_zip(tsv_path: Path, zip_path: Path, zip_url: str) -> None:
    """Ensure a TSV exists by downloading/extracting its own corresponding zip file."""
    if tsv_path.exists():
        print(f"  Found existing {tsv_path.name} — skipping download.")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not zip_path.exists():
        download(zip_url, zip_path)
    else:
        print(f"  Zip already downloaded: {zip_path.name}")

    extract_zip(zip_path, RAW_DIR)
    remove_zip(zip_path)
    if not tsv_path.exists():
        raise FileNotFoundError(
            f"Extraction succeeded but {tsv_path.name} was not found. "
            f"Expected from zip: {zip_path.name}"
        )


def load_name_map(tsv_path: Path) -> dict[str, str]:
    """Return {disambig_assignee_id: organization_name} from the TSV."""
    print(f"  Reading name map from {tsv_path.name}…")

    # Peek at actual column names (the file may use different casing)
    header = pd.read_csv(tsv_path, sep="\t", nrows=0, engine="python")
    cols = [c.strip().strip('"').lower() for c in header.columns]

    id_col = next((c for c in cols if c == "assignee_id" or "disambig_assignee_id" in c), None)
    # Prefer the organization column; fall back to individual name columns only if absent
    name_col = next((c for c in cols if c == "disambig_assignee_organization"), None)
    if not name_col:
        name_col = next((c for c in cols if "organization" in c), None)
    if not name_col:
        name_col = next((c for c in cols if "assignee_name" in c), None)

    if not id_col or not name_col:
        raise ValueError(
            f"Could not find expected columns in {tsv_path.name}.\n"
            f"Available columns: {cols}"
        )

    print(f"  Using columns: id='{id_col}', name='{name_col}'")

    name_map: dict[str, str] = {}
    for chunk in pd.read_csv(
        tsv_path,
        sep="\t",
        dtype=str,
        usecols=[id_col, name_col],
        engine="python",
        chunksize=200_000,
    ):
        chunk.columns = [c.strip().strip('"').lower() for c in chunk.columns]
        chunk = chunk.dropna(subset=[id_col, name_col])
        chunk = chunk[chunk[name_col].str.strip() != ""]
        name_map.update(dict(zip(chunk[id_col], chunk[name_col])))

    print(f"  Loaded {len(name_map):,} id→name mappings.")
    return name_map


def build_uuid_to_name_map(
    persistent_tsv_path: Path,
    assignee_tsv_path: Path,
) -> dict[str, str]:
    """Map company UUIDs from persistent_assignee to organization names from assignee_disambiguated.
    
    The persistent_assignee.tsv has timestamped UUIDs (disamb_assignee_id_*) keyed by (patent_id, assignee_sequence).
    The assignee_disambiguated.tsv has assignee_id and disambig_assignee_organization keyed by the same.
    We join on patent_id + assignee_sequence to map UUIDs → organization names.
    """
    if not persistent_tsv_path.exists() or not assignee_tsv_path.exists():
        print("  Persistent or assignee TSV not found; skipping UUID→name mapping.")
        return {}

    print("  Building UUID→organization name map via patent_id + assignee_sequence join…")

    # Load persistent assignee file to get latest UUID per (patent_id, assignee_sequence)
    print("    Loading persistent assignee UUIDs…")
    persistent_cols = ["patent_id", "assignee_sequence"] + [
        c for c in pd.read_csv(persistent_tsv_path, sep="\t", nrows=0, engine="python").columns
        if c.startswith("disamb_assignee_id_")
    ]
    persistent_data = []
    for chunk in pd.read_csv(
        persistent_tsv_path,
        sep="\t",
        dtype=str,
        usecols=persistent_cols[:min(3, len(persistent_cols))] + persistent_cols[3:],  # Ensure patent_id, assignee_sequence present
        engine="python",
        chunksize=200_000,
    ):
        chunk.columns = [c.strip().strip('"').lower() for c in chunk.columns]
        # Get the most recent (rightmost) non-empty UUID for each row
        id_cols = [c for c in chunk.columns if c.startswith("disamb_assignee_id_")]
        if id_cols:
            chunk["uuid"] = chunk[id_cols].bfill(axis=1).iloc[:, 0]
            chunk = chunk[["patent_id", "assignee_sequence", "uuid"]].dropna(subset=["uuid"])
            persistent_data.append(chunk)
    
    if not persistent_data:
        print("  No UUIDs found in persistent assignee file.")
        return {}
    
    persistent_df = pd.concat(persistent_data, ignore_index=True)
    print(f"    Loaded {len(persistent_df):,} persistent assignee UUIDs.")

    # Load disambiguated assignee file to get organization names
    print("    Loading assignee organizations…")
    assignee_cols = ["patent_id", "assignee_sequence", "assignee_id", "disambig_assignee_organization"]
    assignee_data = []
    for chunk in pd.read_csv(
        assignee_tsv_path,
        sep="\t",
        dtype=str,
        usecols=assignee_cols,
        engine="python",
        chunksize=200_000,
    ):
        chunk.columns = [c.strip().strip('"').lower() for c in chunk.columns]
        chunk = chunk.dropna(subset=["disambig_assignee_organization"])
        chunk = chunk[chunk["disambig_assignee_organization"].str.strip() != ""]
        assignee_data.append(chunk)
    
    if not assignee_data:
        print("  No organization names found in assignee file.")
        return {}
    
    assignee_df = pd.concat(assignee_data, ignore_index=True)
    print(f"    Loaded {len(assignee_df):,} assignee organization records.")

    # Join on patent_id + assignee_sequence to map UUID → organization
    print("    Joining persistent UUIDs with organizations…")
    merged = persistent_df.merge(
        assignee_df[["patent_id", "assignee_sequence", "disambig_assignee_organization"]],
        on=["patent_id", "assignee_sequence"],
        how="left",
    )
    merged = merged.dropna(subset=["uuid", "disambig_assignee_organization"])
    merged = merged[merged["disambig_assignee_organization"].str.strip() != ""]
    
    # Build UUID → organization name map (prefer first seen name for each UUID)
    uuid_map: dict[str, str] = {}
    for _, row in merged.iterrows():
        uuid = row["uuid"]
        org = row["disambig_assignee_organization"]
        if uuid not in uuid_map:
            uuid_map[uuid] = org
    
    print(f"    Built {len(uuid_map):,} UUID→name mappings.")
    return uuid_map


def update_database(name_map: dict[str, str]) -> int:
    """Update companies.name in the SQLite DB. Returns number of rows updated."""
    if not DB_PATH.exists():
        print(f"  WARNING: database not found at {DB_PATH} — skipping DB update.")
        return 0

    updated = 0
    with sqlite3.connect(DB_PATH) as conn:
        companies = pd.read_sql("SELECT company_id, name FROM companies", conn)
        print(f"  DB has {len(companies):,} companies.")

        rows_to_update = []
        for _, row in companies.iterrows():
            new_name = name_map.get(row["company_id"])
            if new_name and new_name != row["name"]:
                rows_to_update.append((new_name, row["company_id"]))

        if rows_to_update:
            conn.executemany(
                "UPDATE companies SET name = ? WHERE company_id = ?", rows_to_update
            )
            conn.commit()
            updated = len(rows_to_update)
            print(f"  Updated {updated:,} company names in DB.")
        else:
            print("  No matching IDs found in name map — DB unchanged.")
            print("  (ID format mismatch between g_persistent_assignee and g_assignee_disambiguated)")
    return updated


def update_csv(name_map: dict[str, str]) -> int:
    """Update clean_companies.csv. Returns number of rows updated."""
    if not CLEAN_COMPANIES_CSV.exists():
        print(f"  WARNING: {CLEAN_COMPANIES_CSV} not found — skipping CSV update.")
        return 0

    df = pd.read_csv(CLEAN_COMPANIES_CSV, dtype=str)
    original = df["name"].copy()
    df["name"] = df.apply(
        lambda r: name_map.get(r["company_id"], r["name"]), axis=1
    )
    updated = int((df["name"] != original).sum())
    df.to_csv(CLEAN_COMPANIES_CSV, index=False)
    print(f"  Updated {updated:,} company names in clean_companies.csv.")
    return updated


# ── main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich company names with PatentsView organization labels.")
    parser.add_argument(
        "--download-all-zips",
        action="store_true",
        help="Download and extract all known PatentsView TSV ZIP files before enrichment.",
    )
    args = parser.parse_args()

    print("\n=== Enrich Company Names ===\n")

    if args.download_all_zips:
        download_all_zips()

    # Step 1 — Ensure required TSV files are present, each from its own ZIP
    print("[1/3] Ensuring required TSV files from matching ZIP files…")
    ensure_tsv_from_own_zip(ASSIGNEE_TSV, ASSIGNEE_ZIP, ASSIGNEE_ZIP_URL)
    ensure_tsv_from_own_zip(
        PERSISTENT_ASSIGNEE_TSV,
        PERSISTENT_ASSIGNEE_ZIP,
        PERSISTENT_ASSIGNEE_ZIP_URL,
    )

    # Step 2 — Build UUID → name map by joining persistent and disambiguated files
    print("\n[2/3] Building UUID→organization name mapping…")
    uuid_to_name_map = build_uuid_to_name_map(PERSISTENT_ASSIGNEE_TSV, ASSIGNEE_TSV)
    
    if not uuid_to_name_map:
        print("  ERROR: UUID→name map is empty. Nothing to do.")
        sys.exit(1)
    
    print(f"  Final map size: {len(uuid_to_name_map):,} UUIDs.")

    # Step 3 — Patch database and CSV
    print("\n[3/3] Patching database and CSV…")
    db_updated = update_database(uuid_to_name_map)
    csv_updated = update_csv(uuid_to_name_map)

    print("\n=== Done ===")
    if db_updated == 0 and csv_updated == 0:
        print(
            "\nWARNING: No names were updated.\n"
            "Check that the company UUIDs in your database match those in g_persistent_assignee.tsv.\n"
            "Your DB company_id sample:\n"
        )
        if DB_PATH.exists():
            with sqlite3.connect(DB_PATH) as conn:
                sample = pd.read_sql("SELECT company_id FROM companies LIMIT 5", conn)
                print(sample.to_string(index=False))

        print("\nSample UUIDs from g_persistent_assignee.tsv (first 5 rows):")
        hdr = pd.read_csv(PERSISTENT_ASSIGNEE_TSV, sep="\t", nrows=5, engine="python")
        hdr.columns = [c.strip().strip('"').lower() for c in hdr.columns]
        id_cols = [c for c in hdr.columns if c.startswith("disamb_assignee_id_")]
        if id_cols:
            print(hdr[id_cols[-1]].head(5).to_string(index=False))
    else:
        print(f"  DB rows updated  : {db_updated:,}")
        print(f"  CSV rows updated : {csv_updated:,}")
        print("\nRe-open the dashboard to see real company names.")


if __name__ == "__main__":
    main()
