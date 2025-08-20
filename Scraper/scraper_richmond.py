import os
import sys
import json
import csv
import time
from pathlib import Path
from urllib.parse import urljoin
import argparse

import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------- Defaults ----------
BASE_URL = "https://www.richmondfed.org/banking/research_data/fry6_reports"
OUT_JSON = "Richmond_JSON.json"
OUT_CSV = "scraped_richmond_data.csv"
S3_BUCKET = "fed-data-storage"
S3_FOLDER = "Richmond_Documents/"
S3_KEY = S3_FOLDER + OUT_JSON
# ------------------------------

# line-buffer stdout so logs appear immediately
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str):
    print(f"[{ts()}] {msg}", flush=True)

def flush_file(f):
    f.flush()
    try:
        os.fsync(f.fileno())
    except Exception:
        pass

def ensure_outputs():
    if not Path(OUT_JSON).exists():
        with open(OUT_JSON, "w") as jf:
            json.dump([], jf)
    if not Path(OUT_CSV).exists():
        with open(OUT_CSV, "w", newline="") as cf:
            cf.write("RSSD,Year\n")

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str):
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket_name, s3_key)
    log(f"📤 Uploaded to S3: s3://{bucket_name}/{s3_key}")

def get_driver(headless: bool = True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1400,1000")
    return webdriver.Chrome(service=Service(), options=options)

def scrape_year(driver, year: int):
    # Load specific year directly via query param
    url = f"{BASE_URL}?year={year}"
    driver.get(url)

    # Wait for table rows to exist
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
    )

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    log(f"   • Found {len(rows)} rows for {year}")

    pdf_links = []
    csv_rows = []

    for r_idx, row in enumerate(rows, start=1):
        try:
            # Columns: 1) RSSD ID, 2) Holding Company Name, 3) Report Date
            rssd = row.find_element(By.CSS_SELECTOR, "td:nth-child(1)").text.strip()
            report_date = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()

            # Extract year from Report Date (e.g., 06/30/2024 -> 2024). Fallback to the year tab.
            row_year = year
            if report_date and len(report_date.split("/")) == 3:
                try:
                    row_year = int(report_date.split("/")[-1])
                except Exception:
                    row_year = year

            # Try to find a direct PDF link inside the row
            pdf_url = None
            anchors = row.find_elements(By.CSS_SELECTOR, "a[href]")
            for a in anchors:
                href = a.get_attribute("href") or ""
                if href.lower().endswith(".pdf"):
                    pdf_url = href
                    break

            # Fallback: sometimes the Holding Company name cell contains the link
            if not pdf_url:
                try:
                    a2 = row.find_element(By.CSS_SELECTOR, "td:nth-child(2) a[href]")
                    href2 = a2.get_attribute("href") or ""
                    if href2.lower().endswith(".pdf"):
                        pdf_url = href2
                except Exception:
                    pass

            # As a last resort, skip if no PDF link found
            if not pdf_url:
                log(f"      ⚠️  No .pdf link found in row {r_idx} ({rssd}, {report_date}); skipping.")
                continue

            # Normalize to absolute URL (usually already absolute on Richmond)
            if pdf_url.startswith("/"):
                pdf_url = urljoin("https://www.richmondfed.org", pdf_url)

            # Append CSV row (real-time write + echo)
            csv_rows.append((rssd, str(row_year)))

            with open(OUT_CSV, "a", newline="") as cf:
                writer = csv.writer(cf)
                for rssd_val, y_val in [csv_rows[-1]]:  # write the just-added one
                    writer.writerow([rssd_val, y_val])
                flush_file(cf)
            log(f"🟢 {OUT_CSV} += {rssd},{row_year}")

            # Append JSON URL (real-time write)
            with open(OUT_JSON, "r+", encoding="utf-8") as jf:
                existing = json.load(jf)
                jf.seek(0)
                existing.append(pdf_url)
                json.dump(existing, jf, indent=2)
                jf.truncate()
            log(f"🔗 URL captured ({year}) → {pdf_url}")

            pdf_links.append(pdf_url)

        except Exception as e:
            log(f"      ❌ Row {r_idx} error: {e}")

    return pdf_links, csv_rows

def main():
    parser = argparse.ArgumentParser(description="Scrape Richmond FR Y-6 PDF URLs + (RSSD,Year) with real-time CSV/JSON output.")
    parser.add_argument("--from-year", type=int, default=2024, help="Start year (inclusive).")
    parser.add_argument("--to-year", type=int, default=2019, help="End year (inclusive).")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless.")
    parser.add_argument("--no-s3", action="store_true", help="Disable S3 upload of the JSON file after each year.")
    parser.add_argument("--s3-bucket", default=S3_BUCKET, help="S3 bucket name.")
    parser.add_argument("--s3-key", default=S3_KEY, help="S3 key for the JSON file.")
    args = parser.parse_args()

    ensure_outputs()
    driver = get_driver(headless=args.headless or True)  # default to headless

    # Iterate years from from_year down to to_year
    start_year = args.from_year
    end_year = args.to_year
    if start_year < end_year:
        year_range = range(start_year, end_year + 1)
    else:
        year_range = range(start_year, end_year - 1, -1)

    try:
        all_count = 0
        for year in year_range:
            log(f"🗓️  Year {year}: loading…")
            links, rows = scrape_year(driver, year)
            all_count += len(links)
            log(f"✅ Year {year}: {len(links)} PDF links captured, {len(rows)} CSV rows written.")

            # Upload updated JSON to S3 after each year if enabled
            if not args.no_s3:
                try:
                    upload_to_s3(OUT_JSON, args.s3_bucket, args.s3_key)
                except Exception as e:
                    log(f"⚠️  S3 upload failed for year {year}: {e}")

        log(f"🏁 Done. Total PDF URLs: {all_count}. See {OUT_JSON} and {OUT_CSV}.")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
