# cleveland_mistral_ocr_upload_bytes.py
import os
import csv
import json
import traceback
from pathlib import Path
from urllib.parse import unquote

import boto3
from dotenv import load_dotenv
from mistralai import DocumentURLChunk, FileTypedDict, Mistral

# ---------------- Config ----------------
BUCKET_NAME = "fed-data-storage"
INPUT_PREFIX = "Cleveland_Documents/"   # PDFs live here (regular S3 objects)
OUTPUT_PREFIX = "Cleveland_Mistral/"    # where OCR JSON will be uploaded

OUTPUT_DIR = Path("./json_cleveland")   # local temp dir for JSONs
PROCESSED_FILE = "processed_files_cleveland.csv"
FAILED_FILE = "failed_files_cleveland.csv"

INCLUDE_IMAGE_B64 = True                # set False for smaller JSON output
# ----------------------------------------

load_dotenv()
api_key = os.getenv("MISTRAL_API_KEY")
if not api_key:
    raise RuntimeError("MISTRAL_API_KEY not found in environment or .env")

client = Mistral(api_key=api_key)
s3 = boto3.client("s3")

def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_processed() -> set[str]:
    if not Path(PROCESSED_FILE).exists():
        return set()
    with open(PROCESSED_FILE, newline="") as f:
        return {row[0] for row in csv.reader(f) if row}

def mark_processed(key: str):
    with open(PROCESSED_FILE, "a", newline="") as f:
        csv.writer(f).writerow([key])

def log_failure(key: str, error_msg: str):
    header_needed = not Path(FAILED_FILE).exists()
    with open(FAILED_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if header_needed:
            w.writerow(["s3_key", "error_message"])
        w.writerow([key, error_msg])

def list_pdf_keys(bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                keys.append(key)
    return keys

def base_name_from_key(key: str) -> str:
    name = key.split("/")[-1]
    name = unquote(name)
    if name.lower().endswith(".pdf"):
        name = name[:-4]
    # sanitize a bit
    return "".join(c for c in name if c not in r'<>:"/\|?*') or "output"

def read_pdf_bytes(bucket: str, key: str) -> bytes:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()

def run_mistral_ocr_from_bytes(pdf_bytes: bytes, base_name: str) -> dict:
    # Upload PDF bytes to Mistral first (purpose="ocr")
    file_dict: FileTypedDict = {"file_name": f"{base_name}.pdf", "content": pdf_bytes}
    uploaded = client.files.upload(file=file_dict, purpose="ocr")

    # Get a short-lived URL for the uploaded file (Mistral API pattern)
    signed = client.files.get_signed_url(file_id=uploaded.id, expiry=60)

    # Now call OCR using that Mistral-hosted URL
    resp = client.ocr.process(
        document=DocumentURLChunk(document_url=signed.url),
        model="mistral-ocr-latest",
        include_image_base64=INCLUDE_IMAGE_B64,
    )
    return json.loads(resp.model_dump_json())

def upload_json(local_path: Path, bucket: str, prefix: str):
    key = f"{prefix}{local_path.name}"
    s3.upload_file(str(local_path), bucket, key)
    return key

def main():
    ensure_dirs()
    processed = load_processed()

    pdf_keys = list_pdf_keys(BUCKET_NAME, INPUT_PREFIX)
    print(f"Found {len(pdf_keys)} PDFs in s3://{BUCKET_NAME}/{INPUT_PREFIX}")

    for key in pdf_keys:
        if key in processed:
            print(f"⏭️  Skipping (already processed): {key}")
            continue

        try:
            base = base_name_from_key(key)
            print(f"📄 Processing: s3://{BUCKET_NAME}/{key}  ->  {base}.json")

            # 1) Read PDF from S3 into memory
            pdf_bytes = read_pdf_bytes(BUCKET_NAME, key)

            # 2) Upload to Mistral & OCR
            result = run_mistral_ocr_from_bytes(pdf_bytes, base)

            # 3) Save locally
            out_path = OUTPUT_DIR / f"{base}.json"
            out_path.write_text(json.dumps(result, indent=2))

            # 4) Upload JSON to S3
            uploaded_key = upload_json(out_path, BUCKET_NAME, OUTPUT_PREFIX)
            print(f"✅ Uploaded OCR JSON → s3://{BUCKET_NAME}/{uploaded_key}")

            # 5) Mark processed & clean up local
            mark_processed(key)
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass

        except Exception as e:
            msg = f"{e.__class__.__name__}: {e}"
            print(f"❌ Failed: {key} — {msg}")
            traceback.print_exc()
            log_failure(key, msg)

    print("🏁 Done.")

if __name__ == "__main__":
    main()
