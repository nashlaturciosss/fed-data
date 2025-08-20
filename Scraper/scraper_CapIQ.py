from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import glob
import boto3
import os
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import csv
from dotenv import load_dotenv
load_dotenv()
import json

# If you want to add a range do list(range(1, 20)) — scrapes pages 1–20
#pages_to_scrape = [1]
pages_to_scrape = list(range(1287, 2287))  # pages 1 through 5766
file = open("failed_pages.csv", "w", newline="")
csv_writer = csv.writer(file)
csv_writer.writerow(["page", "error"])


def wait_for_zip_or_error(download_dir, timeout=120):
    print("⏳ Waiting for ZIP to appear and stabilize...")
    start_time = time.time()
    previous_zips = set(glob.glob(os.path.join(download_dir, "*.zip")))

    while time.time() - start_time < timeout:
        current_zips = set(glob.glob(os.path.join(download_dir, "*.zip")))
        new_zips = current_zips - previous_zips
        crdownloads = glob.glob(os.path.join(download_dir, "*.crdownload"))

        if new_zips and not crdownloads:
            newest = max(new_zips, key=os.path.getctime)
            time.sleep(2)
            if time.time() - os.path.getmtime(newest) > 2:
                return "success"

        try:
            error_element = driver.find_element(By.XPATH,
                                                '//p[contains(text(), "Error occurred while preparing documents")]')
            if error_element.is_displayed():
                return "error"
        except:
            pass

        try:
            pending_element = driver.find_element(By.XPATH, '//p[contains(text(), "are being compressed as zip")]')
            if pending_element.is_displayed():
                print("ℹ️ Still compressing ZIP...")
        except:
            pass

        time.sleep(1)

    return "timeout"


def wait_for_loading_to_finish(timeout=30):
    try:
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located((By.XPATH, '//div[@data-testid="loading-indicator"]'))
        )
    except:
        print("⚠️ Loading overlay did not disappear in time.")


def wait_for_toasts_to_disappear(timeout=10):
    try:
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located((By.XPATH, '//div[contains(@class, "Toastify")]'))
        )
    except:
        print("⚠️ Toast messages did not disappear in time.")


def retry_scroll_and_click(driver, element, max_attempts=3):
    for attempt in range(max_attempts):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            ActionChains(driver).move_to_element(element).click().perform()
            return True
        except Exception as e:
            print(f"🔁 Retry {attempt + 1} for scroll & click: {e}")
            time.sleep(2)
    return False


# === SETUP CHROME ===
download_dir = os.path.join(os.getcwd(), "capitaliq_downloads")
os.makedirs(download_dir, exist_ok=True)

options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=options)

# === LOAD COOKIES FROM .env ===
cookies = os.getenv("COOKIES")
try:
    cookies = json.loads(cookies)
except json.JSONDecodeError as e:
    print(f"❌ Failed to parse cookies: {e}")
    cookies = []

# --- Add cookies for spglobal.com ---
driver.get("https://www.spglobal.com")
time.sleep(2)
for cookie in cookies:
    if "spglobal.com" in cookie["domain"] and "capitaliq" not in cookie["domain"]:
        try:
            driver.add_cookie({
                "name": cookie["name"],
                "value": cookie["value"],
                "domain": cookie["domain"],
                "path": cookie.get("path", "/"),
                "secure": cookie.get("secure", False),
                "httpOnly": cookie.get("httpOnly", False),
            })
        except Exception as e:
            print(f"⚠️ Cookie failed (spglobal): {cookie['name']} — {e}")

# --- Add cookies for capitaliq.spglobal.com ---
driver.get("https://www.capitaliq.spglobal.com")
time.sleep(2)
for cookie in cookies:
    if "capitaliq.spglobal.com" in cookie["domain"]:
        try:
            driver.add_cookie({
                "name": cookie["name"],
                "value": cookie["value"],
                "domain": cookie["domain"],
                "path": cookie.get("path", "/"),
                "secure": cookie.get("secure", False),
                "httpOnly": cookie.get("httpOnly", False),
            })
        except Exception as e:
            print(f"⚠️ Cookie failed (capitaliq): {cookie['name']} — {e}")

# === NOW GO TO THE TARGET PAGE ===
driver.get("https://www.capitaliq.spglobal.com/apisv3/spg-webplatform-core/search/searchResults?vertical=institutional_filingsrpt-gss")
wait = WebDriverWait(driver, 10)
time.sleep(5)


try:
    filing_date_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[contains(@class, "css-1swziob") and @title="Select Filing Date"]')
    ))
    filing_date_btn.click()
    print("✅ Clicked 'Select Filing Date'.")
except Exception as e:
    print(f"❌ Step 1 failed (click 'Select Filing Date'): {e}")

# Step 2 (Final): Click the div with title "Custom" and class "css-684y9u"
try:
    custom_div = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//div[@title="Custom" and contains(@class, "css-684y9u")]')
    ))
    custom_div.click()
    print("✅ Clicked Custom date range option.")
except Exception as e:
    print(f"❌ Step 2 failed (click div with title='Custom'): {e}")

try:
    from_input = wait.until(EC.presence_of_element_located(
        (By.NAME, "date-range-selector-from-value")
    ))
    ActionChains(driver).move_to_element(from_input).click().perform()
    time.sleep(0.3)
    from_input.clear()
    for _ in range(10):
        from_input.send_keys(Keys.BACKSPACE)
    from_input.send_keys("01/01/1995", Keys.ENTER)
    print("✅ Step 4: Set 'From' date.")
except Exception as e:
    print(f"❌ Step 4 failed: {e}")

time.sleep(3)

# Step 5: 'To' date input (type="input", name="date-range-selector-to-value")
try:
    to_input = wait.until(EC.presence_of_element_located(
        (By.NAME, "date-range-selector-to-value")
    ))
    ActionChains(driver).move_to_element(to_input).click().perform()
    time.sleep(0.3)
    to_input.clear()
    for _ in range(10):
        from_input.send_keys(Keys.BACKSPACE)
    to_input.send_keys("07/31/2025", Keys.ENTER)
    print("✅ Step 5: Set 'To' date.")
except Exception as e:
    print(f"❌ Step 5 failed: {e}")

time.sleep(3)

# Step 6: Click the "Done" button
try:
    done_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[contains(@class, "css-d27mz") and .//span[text()="Done"]]')
    ))
    done_button.click()
    print("✅ Clicked 'Done' to confirm date range.")
except Exception as e:
    print(f"❌ Step 6 failed (click 'Done' button): {e}")

# Step 4: Click "Select Document Type"
try:
    select_doc_type = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//span[text()="Select Document Type"]'))
    )
    select_doc_type.click()
    print("✅ Clicked 'Select Document Type'.")
except Exception as e:
    print("❌ Failed to click 'Select Document Type':", e)

# Step 5: Click the caret button next to "Institutional Filings"
try:
    institutional_caret_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//div[@id="InstitutionalFilings"]//button'))
    )
    institutional_caret_btn.click()
    print("✅ Clicked the caret arrow for Institutional Filings.")
except Exception as e:
    print("❌ Failed to click the caret arrow:", e)

# Step 6: Click the small arrow next to "Bank Regulatory Filings"
try:
    bank_regulatory_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//div[@id="BankRegulatoryFilings"]//button'))
    )
    bank_regulatory_button.click()
    print("✅ Clicked the caret for Bank Regulatory Filings.")
except Exception as e:
    print("❌ Failed to click Bank Regulatory Filings caret:", e)

# Step 7: Click the small arrow next to "Regulatory Filing: Depository"
try:
    reg_depository_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//div[contains(@id, "RegulatoryFiling:Depository")]//button'))
    )
    reg_depository_btn.click()
    print("✅ Clicked the caret for Regulatory Filing: Depository.")
except Exception as e:
    print("❌ Failed to click Regulatory Filing: Depository caret:", e)

# Step 8: Click the checkbox for Y-6
try:
    y6_checkbox = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//div[@id="Y-6"]//label[@data-option-label="true"]'))
    )
    y6_checkbox.click()
    print("✅ Y-6 checkbox selected.")
except Exception as e:
    print("❌ Failed to select Y-6 checkbox:", e)

# Step 8: Click the checkbox for Y-6
try:
    y6_checkbox = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//div[@id="Y-6/A"]//label[@data-option-label="true"]'))
    )
    y6_checkbox.click()
    print("✅ Y-6 checkbox selected.")
except Exception as e:
    print("❌ Failed to select Y-6 checkbox:", e)

# Step 9: Click "Sort by" dropdown and select "Filing Date"
try:
    # Click the "Sort by" dropdown (Relevance)
    sort_dropdown = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//button[@title="Relevance"]'))
    )
    sort_dropdown.click()
    print("✅ Opened sort dropdown.")

    # Wait for the dropdown menu to appear and click "Filing Date"
    filing_date_option = wait.until(
        EC.element_to_be_clickable((By.XPATH, '//div[text()="Filing Date"]'))
    )
    filing_date_option.click()
    print("✅ Selected 'Filing Date' from sort options.")

except Exception as e:
    print("❌ Failed to sort by Filing Date:", e)



# Track pages where download fails
failed_pages = []

def record_failed_page(page, error_msg):
    if page not in [row[0] for row in failed_pages]:
        failed_pages.append((page, error_msg))
        csv_writer.writerow([page, error_msg])
        file.flush()


for page in  pages_to_scrape:
    print(f"\n📄 Processing Page {page}")
    try:
        print("ATTEMPTING TO CLICK ALERTS")
        try:
            alert_elements = WebDriverWait(driver, 3).until(
                lambda d: d.find_elements(By.XPATH, '//p[@role="alert"]')
            )
            for alert in alert_elements:
                # Find the next sibling button (adjust as needed for your DOM)
                print(alert)
                parent = alert.find_element(By.XPATH, '../..')
                # print a list of the children of the parent element
                print("Parent children:", [child.tag_name for child in parent.find_elements(By.XPATH, './*')])
                sibling_btn = parent.find_element(By.XPATH, './/button')
                sibling_btn.click()
                print("✅ Clicked alert sibling button.")
        except Exception as e:
            print(f"⚠️ No alert or sibling button found: {e}")


        # ========== STEP 9.5: Navigate to target page ==========
        try:
            time.sleep(1)
            wait_for_toasts_to_disappear()
            next_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, f'//button[contains(@class, "css-18ydibh css-1gfzd5y") and .//span[text()="Next"]]'))
            )

            while True:
                try:
                    page_btn = driver.find_element(
                        By.XPATH,
                        f'//button[contains(@class, "css-l1fgal") and contains(@class, "css-1gfzd5y") and .//span[text()="{page}"]]'
                    )
                    if page_btn.is_displayed():
                        break
                except:
                    page_btn = None

                print(f"➡️ Page {page} button not found, clicking 'Next' instead.")
                next_btn.click()
                time.sleep(1)

            if retry_scroll_and_click(driver, page_btn):
                print(f"➡️ Navigated to page {page}")
            else:
                print(f"⚠️ Could not navigate to page {page} after retries. Skipping.")
                record_failed_page(page, e)
                continue

        except Exception as e:
            print(f"❌ Failed to navigate to page {page}: {e}")
            record_failed_page(page, e)
            continue

        wait_for_loading_to_finish()
        wait_for_toasts_to_disappear()

        # ========== Step 10: Select all ==========
        for _ in range(3):
            try:
                select_all_checkbox = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, '//div[@class="css-5j5vrg"]//label[@data-option-label="true"]')))
                select_all_checkbox.click()
                print("✅ Selected all PDFs on page.")
                break
            except:
                print("🔁 Retrying select-all checkbox...")
                time.sleep(2)

        # ========== Step 11–13: Download + upload to S3 ==========
        wait_for_toasts_to_disappear()
        three_dot_menu = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@title="Multi-Select Actions"]')))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", three_dot_menu)
        time.sleep(1)
        three_dot_menu.click()
        print("✅ Opened 3-dot menu.")

        download_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, '//span[text()="Download Checked Files"]')))
        download_btn.click()
        print("⬇️ Clicked 'Download Checked Files'.")

        result = wait_for_zip_or_error(download_dir)

        if result == "success":
            print("✅ ZIP download complete. Uploading to S3...")

            zip_files = glob.glob(os.path.join(download_dir, "*.zip"))
            if zip_files:
                newest_zip = max(zip_files, key=os.path.getctime)
                s3_key = f"UpdateDocuments/{os.path.basename(newest_zip)}"
                try:
                    boto3.client('s3').upload_file(newest_zip, "fed-data-storage", s3_key)
                    print(f"✅ Uploaded {newest_zip} to S3.")
                    os.remove(newest_zip)
                except Exception as e:
                    print(f"❌ S3 upload failed: {e}")
                    record_failed_page(page, "S3 upload failed: " + str(e))
            else:
                print("❌ No ZIP found after download.")
                record_failed_page(page, "No ZIP file found after download.")

        elif result == "error":
            print(f"⚠️ Document preparation error on page {page}.")
            record_failed_page(page, "Document preparation error")

        else:  # timeout
            print(f"❌ Timeout on page {page}.")
            record_failed_page(page, "Timeout waiting for ZIP download")

        time.sleep(2)

    except Exception as e:
        print(f"❌ Unhandled error on page {page}: {e}")
        record_failed_page(page, "Unhandled error: " + str(e))
        continue
