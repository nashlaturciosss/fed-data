import os
import json
import time
import pandas as pd
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# AWS setup
bucket_name = "fed-data-storage"
s3_folder = "Dallas_Documents/"
json_file = "Dallas_JSON.json"
s3_key = s3_folder + json_file

def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket_name, s3_key)
    print(f"📤 Uploaded to S3: s3://{bucket_name}/{s3_key}")

# Setup Chrome driver
options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(), options=options)

# Target URL
start_url = "https://www.dallasfed.org/banking/nic/fry-6"
driver.get(start_url)

# Output files
csv_file = "scraped_dallas_data.csv"

# Create output files if they don't exist
if not os.path.exists(csv_file):
    with open(csv_file, "w") as f:
        f.write("DocumentID,Year\n")

if not os.path.exists(json_file):
    with open(json_file, "w") as f:
        json.dump([], f)

page = 1

while True:
    print(f"Scraping page {page}...")

    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tbody tr"))
    )

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    new_csv_rows = []
    new_json_links = []

    for row in rows:
        try:
            doc_link = row.find_element(By.CSS_SELECTOR, "td:nth-child(1) a")
            doc_id = doc_link.text.strip()
            doc_url = doc_link.get_attribute("href")
            year = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()

            new_csv_rows.append(f"{doc_id},{year}\n")
            new_json_links.append(doc_url)
        except Exception as e:
            print("Error on row:", e)
            continue

    with open(csv_file, "a") as f:
        f.writelines(new_csv_rows)

    with open(json_file, "r+") as f:
        existing_links = json.load(f)
        f.seek(0)
        updated_links = existing_links + new_json_links
        json.dump(updated_links, f, indent=2)
        f.truncate()

    upload_to_s3(json_file, bucket_name, s3_key)

    # Try to click the "Next" button
    try:
        next_btn = driver.find_element(By.CSS_SELECTOR, 'button.page-link.next')
        if "disabled" in next_btn.get_attribute("class"):
            print("Reached last page.")
            break

        driver.execute_script("arguments[0].click();", next_btn)
        page += 1
        time.sleep(1.5)
    except Exception as e:
        print(f"Failed to click next: {e}")
        break

driver.quit()
print("✅ Finished scraping and uploading JSON.")
