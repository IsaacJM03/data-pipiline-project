"""
USPTO Bulk Data Pipeline
Download → Validate → Extract ZIP files
"""

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import zipfile
import time
import re
import json
from tqdm import tqdm

# ================= CONFIG ================= #

API_URL = "https://data.uspto.gov/ui/datasets/products/pvgpatdis"

FROM_DATE = "1976-01-01"
TO_DATE = "2025-09-30"

OUTPUT_DIR = Path("data/raw/uspto/zips")
EXTRACT_DIR = Path("data/raw/uspto/extracted")

MAX_WORKERS = 2
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB
RETRIES = 3

FILTER_PATTERN = None  # e.g. r"patent|inventor"

# ========================================== #

SESSION = requests.Session()
ADAPTER = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
SESSION.mount("https://", ADAPTER)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://data.uspto.gov/bulkdata/datasets/pvgpatdis",
    "Connection": "keep-alive"
}
from playwright.sync_api import sync_playwright


def get_session_cookies():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Load page to establish session
        page.goto("https://data.uspto.gov/bulkdata/datasets/pvgpatdis")
        page.wait_for_timeout(3000)

        cookies = context.cookies()
        browser.close()

    # Convert to requests format
    return {cookie["name"]: cookie["value"] for cookie in cookies}


# ================= FETCH ================= #

def fetch_metadata():
    params = {
        "includeFiles": "true",
        "fileDataFromDate": FROM_DATE,
        "fileDataToDate": TO_DATE
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://data.uspto.gov/bulkdata/datasets/pvgpatdis",
        "Origin": "https://data.uspto.gov",
        "Connection": "keep-alive"
    }

    r = requests.get(API_URL, params=params, headers=headers, timeout=60)

    # 🔥 DEBUG: print if blocked
    if "application/json" not in r.headers.get("content-type", ""):
        print("⚠️ Blocked by USPTO. Response preview:")
        print(r.text[:500])
        # Keep pipeline runnable even when API response is blocked by WAF.
        return {"bulkDataProductBags": []}

    return r.json()


def extract_files(payload):
    files = []

    products = payload.get("bulkDataProductBags") or payload.get("bulkDataProductBag") or []

    for product in products:
        file_bag = product.get("bulkDataFileBag") or product.get("productFileBag") or {}

        for item in file_bag.get("fileDataBag", []):
            name = item.get("fileName")
            url = item.get("fileDownloadURI")
            size = item.get("fileSize")

            if not name or not url:
                continue

            # Only ZIP files
            if not name.lower().endswith(".zip"):
                continue

            if FILTER_PATTERN and not re.search(FILTER_PATTERN, name, re.I):
                continue

            if isinstance(url, str) and url.startswith("http"):
                full_url = url
            else:
                full_url = "https://data.uspto.gov" + str(url)

            files.append({
                "name": name,
                "url": full_url,
                "size": int(size) if size else None
            })

    return files

# ================= DOWNLOAD ================= #

from playwright.sync_api import sync_playwright
import time

def download_file(file):
    path = OUTPUT_DIR / file["name"]

    # Skip if already correct
    if path.exists() and file["size"]:
        if path.stat().st_size == file["size"]:
            return f"SKIPPED {file['name']}"

    for attempt in range(RETRIES):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()

                # IMPORTANT: load dataset page first (sets cookies/session)
                page.goto("https://data.uspto.gov/bulkdata/datasets/pvgpatdis")
                page.wait_for_timeout(2000)  # Give cookies time to set

                # Trigger real download
                with page.expect_download(timeout=120000) as dl_info:
                    page.goto(file["url"])

                download = dl_info.value
                download.save_as(path)

                browser.close()

            # Validate after download
            if not path.exists():
                raise ValueError("download file not found")
                
            if file["size"] and path.stat().st_size != file["size"]:
                raise ValueError(
                    f"size mismatch expected={file['size']} got={path.stat().st_size}"
                )
                
            if not zipfile.is_zipfile(path):
                raise ValueError("download is not a valid ZIP file")

            return f"DOWNLOADED {file['name']}"

        except Exception as e:
            if attempt == RETRIES - 1:
                return f"FAILED {file['name']} → {e}"
            time.sleep(2 ** attempt)

# ================= EXTRACT ================= #

def extract_zip(file):
    zip_path = OUTPUT_DIR / file["name"]
    extract_path = EXTRACT_DIR / file["name"].replace(".zip", "")

    if extract_path.exists():
        return f"EXTRACT SKIPPED {file['name']}"

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_path)

        return f"EXTRACTED {file['name']}"

    except Exception as e:
        return f"EXTRACT FAILED {file['name']} → {e}"


# ================= PIPELINE ================= #

def run_pipeline():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n📡 Fetching metadata...")
    payload = fetch_metadata()

    print("📦 Extracting file list...")
    files = extract_files(payload)

    print(f"✅ Found {len(files)} ZIP files\n")

    # ---- DOWNLOAD ---- #
    print("⬇️ Starting downloads...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_file, f) for f in files]

        for f in as_completed(futures):
            print(f.result())

    # ---- EXTRACT ---- #
    print("\n📂 Extracting files...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(extract_zip, f) for f in files]

        for f in as_completed(futures):
            print(f.result())

    print("\n🎉 Pipeline complete!")


# ================= RUN ================= #

if __name__ == "__main__":
    run_pipeline()