import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

BUCKET = "fed-data-storage"
PREFIX = "ScrapedDistrictData/"  # S3 “folder” (object key prefix)

FILES = [
    "scraped_cleveland_data.csv",
    "scraped_dallas_data.csv",
    "scraped_richmond_data.csv",
    "scraped_minneapolis_data.csv",   # will be skipped if missing
    "cleveland_failed_scraping.csv",
]

def main():
    s3 = boto3.client("s3")
    base = Path.cwd()
    for fname in FILES:
        p = base / fname
        if not p.exists():
            print(f"⚠️  Skipping (not found): {fname}")
            continue
        key = f"{PREFIX}{p.name}"
        try:
            print(f"⬆️  Uploading {p} -> s3://{BUCKET}/{key}")
            s3.upload_file(
                Filename=str(p),
                Bucket=BUCKET,
                Key=key,
                ExtraArgs={"ContentType": "text/csv"}
            )
            print(f"✅ Uploaded: s3://{BUCKET}/{key}")
        except ClientError as e:
            print(f"❌ Failed to upload {fname}: {e}")

if __name__ == "__main__":
    main()
