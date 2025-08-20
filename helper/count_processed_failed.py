import csv
import io
import posixpath
import sys
import boto3
from botocore.exceptions import ClientError

BUCKET = "fed-data-storage"
PREFIX = "ProcessedMistral/"   # S3 "folder"
HAS_HEADERS = True             # set False if your CSVs have no header

def count_csv_rows(s3, bucket: str, key: str, has_headers: bool = True) -> int:
    """Stream a CSV from S3 and count rows. Subtract 1 for header if has_headers."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        print(f"⚠️  Could not read {key}: {e}", file=sys.stderr)
        return 0

    # Wrap the streaming body as text
    text_stream = io.TextIOWrapper(obj["Body"], encoding="utf-8", newline="")
    reader = csv.reader(text_stream)

    row_count = sum(1 for _ in reader)
    if has_headers and row_count > 0:
        row_count -= 1
    return max(row_count, 0)

def main():
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    processed_total = 0
    failed_total = 0

    processed_breakdown = []
    failed_breakdown = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue

            base = posixpath.basename(key).lower()
            if base.startswith("processed"):
                n = count_csv_rows(s3, BUCKET, key, HAS_HEADERS)
                processed_total += n
                processed_breakdown.append((key, n))
            elif base.startswith("failed"):
                n = count_csv_rows(s3, BUCKET, key, HAS_HEADERS)
                failed_total += n
                failed_breakdown.append((key, n))

    print("====== Totals ======")
    print(f"Processed rows total: {processed_total}")
    print(f"Failed rows total:    {failed_total}")

    # Optional per-file breakdown (uncomment if you want details)
    # print("\n-- Processed files --")
    # for k, n in processed_breakdown:
    #     print(f"{n:>6}  {k}")
    # print("\n-- Failed files --")
    # for k, n in failed_breakdown:
    #     print(f"{n:>6}  {k}")

if __name__ == "__main__":
    main()
