# scraper_cleveland.py
import os
import sys
import time
import glob
import csv
import argparse
import itertools
from pathlib import Path
from typing import Set, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

# Optional S3 upload
try:
    import boto3
    BOTO3 = True
except Exception:
    boto3 = None  # type: ignore
    BOTO3 = False


BASE_URL = "https://www.clevelandfed.org/banking-and-payments/fry6-reports"

# ----------- real-time logging -----------
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str):
    print(f"[{ts()}] {msg}", flush=True)

def fsync_file(f):
    f.flush()
    try:
        os.fsync(f.fileno())
    except Exception:
        pass
# -----------------------------------------


def make_driver(download_dir: str, headless: bool = True) -> webdriver.Chrome:
    """
    We still set a download dir, but downloads happen via HTTP (requests).
    """
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    opts = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        # force Chrome to download PDFs if anything gets clicked by accident
        "plugins.always_open_pdf_externally": True,
        "download.open_pdf_in_system_reader": False,
    }
    opts.add_experimental_option("prefs", prefs)
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1200")
    return webdriver.Chrome(service=Service(), options=opts)


def expand_year_and_get_panel(driver: webdriver.Chrome, year: int):
    """
    Click the 'FR Y-6 Reports {year}' header if needed, then return the content panel WebElement.
    Uses aria-controls to locate the correct panel. Falls back to the next sibling container.
    """
    wait = WebDriverWait(driver, 20)

    header = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        f'//*[self::button or self::a][contains(normalize-space(.), "FR Y-6 Reports {year}")]'
    )))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", header)
    time.sleep(0.2)

    expanded = header.get_attribute("aria-expanded")
    if expanded not in ("true", True):
        ActionChains(driver).move_to_element(header).click().perform()
        time.sleep(0.4)

    panel_id = header.get_attribute("aria-controls")
    if panel_id:
        panel = driver.find_element(By.ID, panel_id)
    else:
        panel = header.find_element(By.XPATH, "following::*[self::div or self::section][1]")
    return panel


def list_pdf_anchors(panel) -> List[Tuple[str, str]]:
    """
    Inside the opened panel, return a list of (text, href) for anchors that are actual PDFs:
    only hrefs ending with .pdf (case-insensitive).
    """
    anchors = panel.find_elements(By.XPATH, './/a[@href]')
    out: List[Tuple[str, str]] = []
    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
            if href.lower().endswith(".pdf"):
                out.append((a.text.strip(), href))
        except Exception:
            continue
    return out


def safe_filename_from_url(url: str) -> str:
    path = unquote(urlparse(url).path)
    name = os.path.basename(path) or "download.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    # sanitize for filesystem
    return "".join(c for c in name if c not in r'<>:"/\|?*')


def http_download_pdf(url: str, dest_dir: str, referer: str, timeout: int = 60, retries: int = 3) -> str:
    """
    Download a PDF via HTTP to dest_dir. Returns local file path on success; raises on failure.
    Validates content-type (best-effort) and size (>1KB).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PDF-Scraper/1.0)",
        "Referer": referer,
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    }
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                ctype = (r.headers.get("Content-Type") or "").lower()

                if ("pdf" not in ctype) and (not url.lower().endswith(".pdf")):
                    raise RuntimeError(f"unexpected content-type: {ctype or 'N/A'}")

                fname = safe_filename_from_url(url)
                out_path = os.path.join(dest_dir, fname)

                # avoid overwrite by suffixing (1), (2), ...
                if os.path.exists(out_path):
                    stem, ext = os.path.splitext(fname)
                    for n in itertools.count(1):
                        alt = os.path.join(dest_dir, f"{stem} ({n}){ext}")
                        if not os.path.exists(alt):
                            out_path = alt
                            break

                size = 0
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            f.write(chunk)
                            size += len(chunk)

                if size < 1024:
                    os.remove(out_path)
                    raise RuntimeError(f"too small: {size} bytes")

                return out_path
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"download failed after {retries} retries: {last_err}")


def upload_to_s3(local_path: str, bucket: str, prefix: str):
    if not BOTO3:
        log("⚠️ boto3 not installed; skipping S3 upload.")
        return
    key = f"{prefix.rstrip('/')}/{os.path.basename(local_path)}"
    boto3.client("s3").upload_file(local_path, bucket, key)
    log(f"📤 Uploaded to s3://{bucket}/{key}")


def ensure_csv_with_header(path: str, headers: List[str]):
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(headers)


def main():
    ap = argparse.ArgumentParser(
        description="Download FR Y-6 PDFs from Cleveland Fed (2013–2023) via HTTP and upload to S3."
    )
    ap.add_argument("--from-year", type=int, default=2023)
    ap.add_argument("--to-year", type=int, default=2013)
    ap.add_argument("--download-dir", default=str(Path.cwd() / "scraped_cleveland_data."))
    ap.add_argument("--s3-bucket", default="fed-data-storage")
    ap.add_argument("--s3-prefix", default="Cleveland_Documents")
    ap.add_argument("--no-s3", action="store_true")
    ap.add_argument("--limit-per-year", type=int, default=0, help="Only download first N PDFs per year (0=all).")
    ap.add_argument("--headless", action="store_true", help="Run Chrome headless (default: headless).")
    ap.add_argument("--debug", action="store_true", help="Print a few sample links per year.")
    args = ap.parse_args()

    # CSVs (real-time appends)
    ensure_csv_with_header("scraped_cleveland_data.csv", ["year", "filename", "href"])
    ensure_csv_with_header("cleveland_failed_scraping.csv", ["year", "item_text", "href", "reason"])
    dl_csv = open("scraped_cleveland_data.csv", "a", newline="", encoding="utf-8")
    fail_csv = open("cleveland_failed_scraping.csv", "a", newline="", encoding="utf-8")
    dl_writer = csv.writer(dl_csv)
    fail_writer = csv.writer(fail_csv)

    driver = make_driver(args.download_dir, headless=True)  # headless by default
    wait = WebDriverWait(driver, 20)

    try:
        log(f"🌐 Opening {BASE_URL}")
        driver.get(BASE_URL)
        time.sleep(2)

        # iterate years (desc by default)
        years = range(args.from_year, args.to_year - 1, -1) if args.from_year >= args.to_year else range(args.from_year, args.to_year + 1)

        for year in years:
            log(f"\n🗓️  Year {year}: expanding…")
            panel = expand_year_and_get_panel(driver, year)
            time.sleep(0.4)  # allow content to render

            pdfs = list_pdf_anchors(panel)
            log(f"   • Found {len(pdfs)} PDF links in {year} (href ends with .pdf)")

            if args.debug and pdfs:
                for t, h in pdfs[:5]:
                    log(f"     ↪ sample: {t or '(no text)'} | {h}")

            processed = 0
            seen_names: Set[str] = set()

            for text, href in pdfs:
                if args.limit_per_year and processed >= args.limit_per_year:
                    break

                try:
                    local_path = http_download_pdf(href, args.download_dir, referer=BASE_URL)
                    fname = os.path.basename(local_path)

                    if fname in seen_names:
                        log(f"↩️  Duplicate skipped: {fname}")
                        try:
                            os.remove(local_path)
                        except Exception:
                            pass
                        continue
                    seen_names.add(fname)

                    log(f"✅ Downloaded: {fname}")
                    dl_writer.writerow([year, fname, href]); fsync_file(dl_csv)

                    if not args.no_s3:
                        try:
                            upload_to_s3(local_path, args.s3_bucket, args.s3_prefix)
                        finally:
                            # remove local copy to save disk
                            try:
                                os.remove(local_path)
                            except Exception:
                                pass

                    processed += 1
                    time.sleep(0.05)

                except Exception as e:
                    fail_writer.writerow([year, text, href, f"http_download_error:{e}"]); fsync_file(fail_csv)
                    log(f"❌ HTTP download error: {href} — {e}")
                    time.sleep(0.05)

            log(f"✅ Year {year}: downloaded {processed} file(s).")

        log("\n🏁 Finished all years.")

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        dl_csv.close()
        fail_csv.close()


if __name__ == "__main__":
    main()
