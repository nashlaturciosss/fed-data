import os
import boto3
import json
import re
import pandas as pd
from pathlib import Path
import csv
from mistralai.models import OCRResponse
import google.generativeai as genai
from dotenv import load_dotenv
load_dotenv()

# === CONFIG ===
bucket_name = "fed-data-testing"
prefix = "json/"
insiders_dir = Path("csv/insiders")
securities_dir = Path("csv/securities")
tracking_csv = Path("gemini_results.csv")
genai.configure(api_key=os.getenv("GENAI_API_KEY"))

# === DIR SETUP ===
insiders_dir.mkdir(parents=True, exist_ok=True)
securities_dir.mkdir(parents=True, exist_ok=True)

# === S3 CLIENT ===
s3 = boto3.client("s3")

def list_all_s3_objects(bucket: str, prefix: str) -> list:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_objects = []
    for page in pages:
        contents = page.get("Contents", [])
        all_objects.extend(contents)
    return all_objects

# === TRACKING ===
def load_tracked_files():
    if not tracking_csv.exists():
        return {}
    with open(tracking_csv, newline="") as f:
        return {row[0]: row[1] for row in csv.reader(f)}

def update_tracking(file: str, status: str, error: str = "", bank_name: str = "", year: str = "", presence: str = ""):
    header_needed = not tracking_csv.exists()
    with open(tracking_csv, "a", newline="") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow(["file", "status", "error", "bank_name", "year"])
        writer.writerow([file, status, error, bank_name, year, presence])


# === HELPER FUNCTIONS ===
def extract_bank_name(markdown: str, filename: str) -> str:
    match = re.search(r"(?i)(?:Legal Title of Holding Company|Reporter's Name.*?)\n+([A-Z0-9 .,&'’\-]+)", markdown)
    if match:
        return match.group(1).strip()
    file_match = re.search(r"([^/\\]+)_Y-6_\d{4}-\d{2}-\d{2}_English", filename)
    return file_match.group(1).replace("_", " ").strip() if file_match else ""

def extract_fiscal_year(markdown: str, filename: str) -> str:
    match = re.search(r"Date of Report.*?:\s*(?:\$)?\s*(\d{2})\s*/\s*(\d{2})\s*/\s*(\d{4})", markdown, re.IGNORECASE) or \
            re.search(r"fiscal year.*?(\d{4})", markdown, re.IGNORECASE)
    if match:
        return match.group(3) if len(match.groups()) == 3 else match.group(1)
    file_match = re.search(r"_Y-6_(\d{4})-\d{2}-\d{2}_English", filename)
    return file_match.group(1) if file_match else ""

def replace_images_in_markdown(markdown_str: str, images_dict: dict) -> str:
    for img_name, base64_str in images_dict.items():
        markdown_str = markdown_str.replace(f"![{img_name}]({img_name})", f"![{img_name}]({base64_str})")
    return markdown_str

def get_combined_markdown(ocr_response: OCRResponse) -> str:
    markdowns = []
    for page in ocr_response.pages:
        image_data = {img.id: img.image_base64 for img in page.images}
        markdowns.append(replace_images_in_markdown(page.markdown, image_data))
    return "\n\n".join(markdowns)


def extract_from_md(md: str, name: str) -> tuple[str, str, str]:
    pdf_name = name

    prompt = f"""
    You are analyzing a U.S. Federal Reserve FR Y-6 regulatory filing.

    From the text below, extract two structured tables and return them as a JSON object with two keys (IF A NONE VALUE IS FOUND—eg. None, N/A, etc—, REPLACE WITH THE VALUE WITH A null value); also include the bank name and fiscal year in the output:

    1. shareholders — list of:
       - Name and Address
       - Country of Citizenship
       - Number and Percentage of Voting Stock

    2. insiders — list of:
       - Name and Address
       - Principal occupation if other than with Bank Holding Company
       - Title and Position with Bank Holding Company
       - Title and Position with Subsidiaries
       - Title and Position with Other Businesses
       - Percentage of Voting Shares in Bank Holding Company
       - Percentage of Voting Shares in Subsidiaries
       - List names of other companies if 25% or more of voting securities are held

    3. bank_data - list of:
         - Bank Name
         - Year

    Return valid JSON only (no markdown or formatting).

    FR Y-6 OCR TEXT:
    ---
    {md}
    """

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    output_text = response.text.strip()

    match = re.search(r'```json\s*({.*?})\s*```', output_text, re.DOTALL) or \
            re.search(r'({.*})', output_text, re.DOTALL)
    if not match:
        raise ValueError("Gemini did not return valid JSON")

    tables = json.loads(match.group(1))

    # Extract bank name and year from Gemini output (still use Gemini for this)
    bank_data_list = tables.get("bank_data", [])
    if bank_data_list and isinstance(bank_data_list, list):
        bank_data = bank_data_list[0]
        bank_name = bank_data.get("Bank Name", "Unknown")
        year = bank_data.get("Year", "Unknown")
    else:
        bank_name = "Unknown"
        year = "Unknown"

    insiders_df = pd.DataFrame(tables.get("insiders", []))
    shareholders_df = pd.DataFrame(tables.get("shareholders", []))

    print("Shareholders", shareholders_df)
    print("Insiders", insiders_df)

    # Table presence computed from actual dataframes
    table_presence = (
        "both" if not insiders_df.empty and not shareholders_df.empty else
        "insiders" if not insiders_df.empty else
        "securities" if not shareholders_df.empty else
        "none"
    )

    base_data = {
        "Bank Name": bank_name,
        "table presence": table_presence,
        "Bank_PDF-Name": pdf_name,
        "Year": year
    }

    if insiders_df.empty:
        insiders_df = pd.DataFrame([base_data])
    else:
        for k, v in base_data.items():
            insiders_df[k] = v

    if shareholders_df.empty:
        shareholders_df = pd.DataFrame([base_data])
    else:
        for k, v in base_data.items():
            shareholders_df[k] = v

    insiders_path = insiders_dir / f"{name}.csv"
    shareholders_path = securities_dir / f"{name}.csv"

    insiders_df.to_csv(insiders_path, index=False)
    shareholders_df.to_csv(shareholders_path, index=False)

    s3.upload_file(str(insiders_path), bucket_name, f"csv/insiders/{name}.csv")
    s3.upload_file(str(shareholders_path), bucket_name, f"csv/securities/{name}.csv")

    # Delete local files after upload
    insiders_path.unlink(missing_ok=True)
    shareholders_path.unlink(missing_ok=True)

    print("Found year:", year)
    print("Found bank name:", bank_name)
    print(f"✅ Saved: insiders/{name}.csv, securities/{name}.csv")

    return bank_name, year, table_presence


# === MAIN DRIVER ===
def main():
    tracked = load_tracked_files()
    objects = objects = list_all_s3_objects(bucket_name, prefix)

    if not objects:
        print("No objects found in S3 bucket.")
        return

    print(f"Found {len(objects)} objects in bucket '{bucket_name}' with prefix '{prefix}'")

    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        name = key.split("/")[-1].replace(".json", "")
        if tracked.get(name) == "passed" or tracked.get(name) == "failed":
            print(f"⏭️ Skipping already processed: {name}")
            continue

        print(f"\n--- Processing: {key} ---")
        try:
            file_obj = s3.get_object(Bucket=bucket_name, Key=key)
            file_content = file_obj["Body"].read().decode("utf-8")
            json_data = json.loads(file_content)
            ocr_response = OCRResponse.model_validate(json_data)
            markdown = get_combined_markdown(ocr_response)

            bank_name, year, presence = extract_from_md(markdown, name)
            update_tracking(name, "passed", bank_name=bank_name, year=year, presence=presence)

        except Exception as e:
            print(f"❌ Failed: {name}: {e}")
            update_tracking(name, "failed", str(e))


if __name__ == "__main__":
    main()
