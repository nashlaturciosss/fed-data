import os
import csv
import json
import traceback
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Set, List

import boto3
from dotenv import load_dotenv
from mistralai import DocumentURLChunk, Mistral

# ------------ Config ------------
DALLAS_JSON_PATH = "Richmond_JSON.json"   # file containing a JSON array of PDF URLs
OUTPUT_DIR = Path("./json")             # local output dir for OCR JSON
PROCESSED_FILE = "processed_files_richmond.csv"  # tracks finished items (by url or name)
FAILED_FILE = "failed_files_richmond.csv"        # logs failures

ENABLE_S3_UPLOAD = True
S3_BUCKET = "fed-data-storage"
S3_PREFIX = "Richmond_Mistral/"
# --------------------------------

load_dotenv()
api_key = os.getenv("MISTRAL_API_KEY")
if not api_key:
    raise RuntimeError("MISTRAL_API_KEY not found in environment or .env")

client = Mistral(api_key=api_key)
s3 = boto3.client("s3") if ENABLE_S3_UPLOAD else None

def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_processed_files() -> Set[str]:
    if not Path(PROCESSED_FILE).exists():
        return set()
    with open(PROCESSED_FILE, newline="") as f:
        return set(row[0] for row in csv.reader(f))

def mark_file_as_processed(identifier: str):
    with open(PROCESSED_FILE, "a", newline="") as f:
        csv.writer(f).writerow([identifier])

def log_failure(identifier: str, error_msg: str):
    header_needed = not Path(FAILED_FILE).exists()
    with open(FAILED_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow(["identifier", "error_message"])
        writer.writerow([identifier, error_msg])

def read_url_list(path: str) -> List[str]:
    with open(path, "r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} must be a JSON array of URLs")
    return data

def name_from_url(url: str) -> str:
    parsed = urlparse(url)
    last = unquote(os.path.basename(parsed.path))
    if last.lower().endswith(".pdf"):
        last = last[:-4]
    return last

def ocr_url_to_json(url: str, base_name: str):
    pdf_response = client.ocr.process(
        document=DocumentURLChunk(document_url=url),
        model="mistral-ocr-latest",
        include_image_base64=True,
    )
    response_dict = json.loads(pdf_response.model_dump_json())

    out_path = OUTPUT_DIR / f"{base_name}.json"
    out_path.write_text(json.dumps(response_dict, indent=2))

    if ENABLE_S3_UPLOAD and s3 is not None:
        s3_key = f"{S3_PREFIX}{out_path.name}"
        s3.upload_file(str(out_path), S3_BUCKET, s3_key)

def main():
    ensure_dirs()
    urls = read_url_list(DALLAS_JSON_PATH)
    # 🔸 removed: urls = urls[:15]

    processed = load_processed_files()
    print(f"Processing {len(urls)} URLs from {DALLAS_JSON_PATH} (all).")

    for url in urls:
        identifier = url.strip()
        if not identifier:
            continue
        if identifier in processed:
            print(f"⏭️  Skipping (already processed): {identifier}")
            continue

        base_name = name_from_url(identifier)
        print(f"📄 Processing: {base_name}")

        try:
            ocr_url_to_json(identifier, base_name)
            mark_file_as_processed(identifier)
            print(f"✅ Done: {base_name}")
        except Exception as e:
            err = f"{e.__class__.__name__}: {e}"
            print(f"❌ Failed: {base_name} — {err}")
            traceback.print_exc()
            log_failure(identifier, err)

if __name__ == "__main__":
    main()
