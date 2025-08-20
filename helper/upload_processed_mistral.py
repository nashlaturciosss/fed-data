from pathlib import Path
import boto3
from botocore.exceptions import ClientError

BUCKET = "fed-data-storage"
PREFIX = "ProcessedMistral/"

FILES = [
    "failed_files_cleveland.csv",
    "failed_files_dallas.csv",
    "failed_files_minneapolis.csv",
    "processed_files_cleveland.csv",
    "processed_files_dallas.csv",
    "processed_files_minneapolis.csv",
    "processed_files_richmond.csv",
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
                ExtraArgs={"ContentType": "text/csv"},
            )
            print(f"✅ Uploaded: s3://{BUCKET}/{key}")
        except ClientError as e:
            print(f"❌ Failed to upload {fname}: {e}")

if __name__ == "__main__":
    main()
