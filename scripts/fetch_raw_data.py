"""
Download, extract, and validate all PatentsView raw data files.

Handles parallel downloads, robust multi-method extraction, and
skips files already present on disk.  Also patches company names
into the database after all files are ready.

Usage:
    python scripts/fetch_raw_data.py              # pipeline files only
    python scripts/fetch_raw_data.py --all        # + optional enrichment files
    python scripts/fetch_raw_data.py --list       # show status, no downloads
    python scripts/fetch_raw_data.py --enrich     # patch company names only
    python scripts/fetch_raw_data.py --extract-only  # extract existing zips and delete them
    python scripts/fetch_raw_data.py --workers 8  # parallel download workers
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

# ── paths ──────────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
RAW_DIR = BASE / "data/raw"
DB_PATH = BASE / "data/patent_analytics.db"
CLEAN_COMPANIES_CSV = BASE / "data/processed/clean_companies.csv"

S3_BASE = "https://s3.amazonaws.com/data.patentsview.org/download"

# ── file manifest ──────────────────────────────────────────────────────────────
# Each entry: (tsv_name, description)
# The zip URL is derived automatically as S3_BASE/{tsv_name}.zip
PIPELINE_FILES: list[tuple[str, str]] = [
    ("g_patent.tsv",                 "Core patent metadata"),
    ("g_patent_abstract.tsv",        "Patent abstracts"),
    ("g_inventor_disambiguated.tsv", "Inventor names and IDs"),
    ("g_location_disambiguated.tsv", "Inventor locations / countries"),
    ("g_persistent_assignee.tsv",    "Patent → assignee ID mapping"),
    ("g_assignee_disambiguated.tsv", "Assignee organization names"),
]

ENRICHMENT_FILES: list[tuple[str, str]] = [
    ("g_cpc_title.tsv",              "CPC technology classification titles"),
    ("g_ipc_at_issue.tsv",           "IPC classification at issue"),
    ("g_gov_interest.tsv",           "Government interest flags"),
    ("g_gov_interest_org.tsv",       "Government organization names"),
    ("g_us_term_of_grant.tsv",       "Patent term of grant"),
    ("g_pct_data.tsv",               "PCT application data"),
]


# ── extraction ─────────────────────────────────────────────────────────────────
def _extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """Extract a zip file, trying multiple methods for robustness."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dest_dir)
        return
    except (zipfile.BadZipFile, Exception):
        pass

    if shutil.which("ditto"):
        r = subprocess.run(
            ["ditto", "-xk", str(zip_path), str(dest_dir)], capture_output=True
        )
        if r.returncode == 0:
            return

    if shutil.which("unzip"):
        r = subprocess.run(
            ["unzip", "-o", str(zip_path), "-d", str(dest_dir)], capture_output=True
        )
        if r.returncode == 0:
            return

    raise RuntimeError(f"All extraction methods failed for {zip_path.name}")


def _remove_zip(zip_path: Path) -> None:
    if zip_path.exists():
        zip_path.unlink()


def _extract_all_zips(raw_dir: Path) -> None:
    """Extract every *.zip in raw_dir and remove it after a successful extraction."""
    zips = sorted(raw_dir.glob("*.zip"))
    if not zips:
        return

    print(f"\n[Extract] {len(zips)} zip file(s) found…")
    for zp in zips:
        tsv_name = zp.stem  # e.g. g_patent.tsv
        tsv_path = raw_dir / tsv_name
        if tsv_path.exists():
            print(f"  SKIP  {zp.name}  (TSV already present)")
            _remove_zip(zp)
            continue
        print(f"  …     {zp.name}", end="", flush=True)
        t0 = time.perf_counter()
        try:
            _extract_zip(zp, raw_dir)
            _remove_zip(zp)
            elapsed = time.perf_counter() - t0
            size_mb = tsv_path.stat().st_size / 1_048_576 if tsv_path.exists() else 0
            print(f"  →  {size_mb:,.0f} MB  ({elapsed:.1f}s)")
        except Exception as exc:
            print(f"  FAIL  ({exc})")


# ── download ───────────────────────────────────────────────────────────────────
def _zip_url(tsv_name: str) -> str:
    return f"{S3_BASE}/{tsv_name}.zip"


def _download_one(tsv_name: str, raw_dir: Path) -> tuple[str, str, float]:
    """Download one zip. Returns (tsv_name, status, elapsed_seconds)."""
    tsv_path = raw_dir / tsv_name
    zip_path = raw_dir / f"{tsv_name}.zip"

    if tsv_path.exists():
        return tsv_name, "skip_tsv", 0.0
    if zip_path.exists():
        return tsv_name, "skip_zip", 0.0

    url = _zip_url(tsv_name)
    t0 = time.perf_counter()

    tool = shutil.which("curl") or shutil.which("wget")
    if not tool:
        raise RuntimeError("curl or wget required")

    if "curl" in tool:
        cmd = ["curl", "-fL", "--retry", "3", "-s", "-o", str(zip_path), url]
    else:
        cmd = ["wget", "-q", "-O", str(zip_path), url]

    result = subprocess.run(cmd, capture_output=True)
    elapsed = time.perf_counter() - t0

    if result.returncode != 0:
        if zip_path.exists():
            zip_path.unlink()
        return tsv_name, f"error ({result.returncode})", elapsed

    size_mb = zip_path.stat().st_size / 1_048_576 if zip_path.exists() else 0
    return tsv_name, f"ok ({size_mb:,.0f} MB)", elapsed


def _download_all(files: list[tuple[str, str]], raw_dir: Path, workers: int) -> None:
    """Download zips for all listed files in parallel."""
    needed = [
        (name, desc) for name, desc in files
        if not (raw_dir / name).exists() and not (raw_dir / f"{name}.zip").exists()
    ]

    if not needed:
        print("[Download] All files already present — nothing to download.")
        return

    print(f"\n[Download] {len(needed)} file(s) to fetch  (workers={workers})…")
    for name, desc in needed:
        print(f"  queued  {name}  —  {desc}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_download_one, name, raw_dir): name
            for name, _ in needed
        }
        for future in as_completed(futures):
            name, status, elapsed = future.result()
            flag = "✓" if status.startswith("ok") else ("→" if "skip" in status else "✗")
            t = f"  {elapsed:.1f}s" if elapsed > 0 else ""
            print(f"  {flag}  {name}  {status}{t}")


# ── status listing ─────────────────────────────────────────────────────────────
def _human_size(path: Path) -> str:
    b = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _list_status(files: list[tuple[str, str]], raw_dir: Path) -> None:
    print(f"\n{'File':<42} {'Status':<14} {'Size'}")
    print("-" * 70)
    for tsv_name, desc in files:
        tsv = raw_dir / tsv_name
        zp = raw_dir / f"{tsv_name}.zip"
        if tsv.exists():
            print(f"  {tsv_name:<40} {'TSV ✓':<14} {_human_size(tsv)}")
        elif zp.exists():
            print(f"  {tsv_name:<40} {'zip only':<14} {_human_size(zp)}")
        else:
            print(f"  {tsv_name:<40} {'missing':<14}")


def _extract_existing_zips_only(raw_dir: Path) -> None:
    """Extract currently present ZIP files in raw_dir and delete them afterward."""
    _extract_all_zips(raw_dir)


# ── company name enrichment ────────────────────────────────────────────────────
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().strip('"').lower() for c in df.columns]
    return df


def _build_name_map(tsv_path: Path) -> dict[str, str]:
    header = _normalize_cols(pd.read_csv(tsv_path, sep="\t", nrows=0, engine="python"))
    cols = list(header.columns)

    id_col = next((c for c in cols if c == "assignee_id" or "disambig_assignee_id" in c), None)
    name_col = next((c for c in cols if c == "disambig_assignee_organization"), None)
    if not name_col:
        name_col = next((c for c in cols if "organization" in c), None)
    if not name_col:
        name_col = next((c for c in cols if "assignee_name" in c), None)

    if not id_col or not name_col:
        raise ValueError(f"Cannot find id/name columns in {tsv_path.name}. Columns: {cols}")

    print(f"  Columns  id='{id_col}'  name='{name_col}'")

    name_map: dict[str, str] = {}
    for chunk in pd.read_csv(
        tsv_path, sep="\t", dtype=str,
        usecols=[id_col, name_col], engine="python", chunksize=500_000,
    ):
        chunk = _normalize_cols(chunk).dropna(subset=[id_col, name_col])
        chunk = chunk[chunk[name_col].str.strip() != ""]
        name_map.update(zip(chunk[id_col], chunk[name_col]))

    return name_map


def _patch_db(name_map: dict[str, str]) -> int:
    if not DB_PATH.exists():
        print(f"  SKIP  DB not found at {DB_PATH}")
        return 0

    with sqlite3.connect(DB_PATH) as conn:
        companies = pd.read_sql("SELECT company_id, name FROM companies", conn)
        updates = [
            (name_map[cid], cid)
            for cid in companies["company_id"]
            if cid in name_map and name_map[cid] != companies.loc[companies["company_id"] == cid, "name"].iloc[0]
        ]
        if updates:
            conn.executemany("UPDATE companies SET name = ? WHERE company_id = ?", updates)
            conn.commit()
    return len(updates)


def _patch_csv(name_map: dict[str, str]) -> int:
    if not CLEAN_COMPANIES_CSV.exists():
        print(f"  SKIP  {CLEAN_COMPANIES_CSV} not found")
        return 0

    df = pd.read_csv(CLEAN_COMPANIES_CSV, dtype=str)
    before = df["name"].copy()
    df["name"] = df.apply(lambda r: name_map.get(r["company_id"], r["name"]), axis=1)
    updated = int((df["name"] != before).sum())
    df.to_csv(CLEAN_COMPANIES_CSV, index=False)
    return updated


def enrich_company_names() -> None:
    tsv = RAW_DIR / "g_assignee_disambiguated.tsv"
    zp = RAW_DIR / "g_assignee_disambiguated.tsv.zip"

    if not tsv.exists():
        if zp.exists():
            print(f"  Extracting {zp.name}…")
            _extract_zip(zp, RAW_DIR)
            _remove_zip(zp)
        else:
            print("  g_assignee_disambiguated.tsv not found — run without --enrich first.")
            return

    print(f"  Reading {tsv.name}…")
    name_map = _build_name_map(tsv)
    print(f"  Loaded {len(name_map):,} id→name mappings")

    db_n = _patch_db(name_map)
    csv_n = _patch_csv(name_map)
    print(f"  DB updated: {db_n:,} rows  |  CSV updated: {csv_n:,} rows")


# ── CLI ────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--all",     action="store_true", help="Also download optional enrichment files")
    p.add_argument("--list",    action="store_true", help="Show file status only, no downloads")
    p.add_argument("--enrich",  action="store_true", help="Patch company names into DB/CSV only")
    p.add_argument("--extract-only", action="store_true", help="Extract zip files already in data/raw and delete them")
    p.add_argument("--workers", type=int, default=4,  help="Parallel download workers (default: 4)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_files = PIPELINE_FILES + (ENRICHMENT_FILES if args.all else [])

    if args.list:
        print("\n── Pipeline files ──")
        _list_status(PIPELINE_FILES, RAW_DIR)
        print("\n── Enrichment files ──")
        _list_status(ENRICHMENT_FILES, RAW_DIR)
        return

    if args.enrich:
        print("\n[Enrich] Patching company names…")
        enrich_company_names()
        return

    if args.extract_only:
        print("\n[Extract] Processing existing ZIP files in data/raw…")
        _extract_existing_zips_only(RAW_DIR)
        print("\nDone.\n")
        return

    # Normal flow: download → extract → enrich
    print(f"\n{'='*55}")
    print(f"  PatentsView Raw Data  —  {len(all_files)} file(s) targeted")
    print(f"  Raw dir: {RAW_DIR}")
    print(f"{'='*55}")

    _download_all(all_files, RAW_DIR, workers=args.workers)
    _extract_all_zips(RAW_DIR)

    # Always patch company names if the enrichment file is available
    assignee_tsv = RAW_DIR / "g_assignee_disambiguated.tsv"
    if assignee_tsv.exists() and DB_PATH.exists():
        print("\n[Enrich] Patching company names…")
        enrich_company_names()

    print("\nDone.\n")


if __name__ == "__main__":
    main()
