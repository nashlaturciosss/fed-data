import boto3
import pandas as pd
from io import StringIO

# Configuration
bucket_name = "fed-data-storage"
prefix = "csv/securities/"
output_csv_name = "all_securities_combined.csv"  # Local output filename

# Initialize S3 client (make sure your AWS CLI is configured)
s3 = boto3.client('s3')

def list_csv_files(bucket, prefix):
    """List all CSV files in a given S3 bucket and prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    csv_keys = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(".csv"):
                csv_keys.append(key)
    return csv_keys

def download_csv_from_s3(bucket, key):
    """Download a CSV file from S3 and return a pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(data))

def main():
    all_csv_keys = list_csv_files(bucket_name, prefix)
    print(f"Found {len(all_csv_keys)} CSV files.")

    all_dfs = []
    for key in all_csv_keys:
        try:
            df = download_csv_from_s3(bucket_name, key)
            all_dfs.append(df)
            print(f"Loaded: {key}")
        except Exception as e:
            print(f"Failed to load {key}: {e}")

    if not all_dfs:
        print("No CSVs loaded. Exiting.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.to_csv(output_csv_name, index=False)
    print(f"Saved combined CSV to: {output_csv_name}")

if __name__ == "__main__":
    main()
