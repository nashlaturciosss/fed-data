import os
import json
import pandas as pd
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# AWS S3 setup
bucket_name = "fed-data-storage"
s3_folder = "Minneapolis_Documents/"
json_file = "Minneapolis_JSON.json"
s3_key = s3_folder + json_file

# Helper: Upload to S3
def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket_name, s3_key)
    print(f"📤 Uploaded to S3: s3://{bucket_name}/{s3_key}")

# Set up Selenium
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # comment this out to see browser
driver = webdriver.Chrome(service=Service(), options=options)

# Start URL
start_url = "https://www.minneapolisfed.org/banking/statistical-and-structure-reports/structure-reports/search-reports"
driver.get(start_url)

# Output file names
csv_file = "scraped_minneapolis_data.csv"

# Create empty files if not exist
if not os.path.exists(csv_file):
    with open(csv_file, "w") as f:
        f.write("RSSD,Year\n")

if not os.path.exists(json_file):
    with open(json_file, "w") as f:
        json.dump([], f)

page = 1
max_pages = 503

while page <= max_pages:
    print(f"Scraping page {page}...")

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
    )

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

    new_json_links = []
    new_csv_rows = []

    for row in rows:
        try:
            link = row.find_element(By.CSS_SELECTOR, "td:nth-child(1) a")
            rssd = link.text.strip()
            href = link.get_attribute("href")
            year = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()

            new_csv_rows.append(f"{rssd},{year}\n")
            new_json_links.append(href)

        except Exception as e:
            print("Error on row:", e)
            continue

    # Append to CSV
    with open(csv_file, "a") as f:
        f.writelines(new_csv_rows)

    # Append to JSON
    with open(json_file, "r+") as f:
        existing_links = json.load(f)
        f.seek(0)
        updated_links = existing_links + new_json_links
        json.dump(updated_links, f, indent=2)
        f.truncate()

    # Upload updated JSON to S3
    upload_to_s3(json_file, bucket_name, s3_key)

    # Go to next page
    try:
        next_button = driver.find_element(By.LINK_TEXT, "Next")
        if "disabled" in next_button.get_attribute("class"):
            break
        next_button.click()
        page += 1
        time.sleep(1.5)
    except Exception as e:
        print("Pagination error:", e)
        break

driver.quit()
print("✅ Scraping complete and JSON uploaded to S3.")
