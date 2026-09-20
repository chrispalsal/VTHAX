#!/usr/bin/env python3
"""
Upload collector.py CSV files to Databricks Unity Catalog volume.
Run this after collecting data with collector.py.

Usage:
    python upload_to_databricks.py data/
    python upload_to_databricks.py data/win1_20260917-140000_measurements.csv

Prerequisites:
    pip install databricks-sdk
    databricks auth login  (or set DATABRICKS_TOKEN env var)
"""
import sys
import os
from pathlib import Path

VOLUME_PATH = "/Volumes/vt_connectivity/campus/csv_uploads"

def upload_file(local_path, volume_path):
    """Upload a single file to the UC volume."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    
    with open(local_path, "rb") as f:
        w.files.upload(volume_path, f, overwrite=True)
    print(f"  ✓ Uploaded {Path(local_path).name} -> {volume_path}")

def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <data_dir_or_file>")
        sys.exit(1)
    
    target = Path(sys.argv[1])
    if not target.exists():
        print(f"Error: {target} does not exist")
        sys.exit(1)
    
    # Find all CSV files
    if target.is_dir():
        csvs = sorted(target.glob("*.csv"))
    else:
        csvs = [target]
    
    if not csvs:
        print("No CSV files found")
        sys.exit(1)
    
    print(f"Uploading {len(csvs)} CSV file(s) to {VOLUME_PATH}/")
    for csv in csvs:
        remote = f"{VOLUME_PATH}/{csv.name}"
        try:
            upload_file(str(csv), remote)
        except Exception as e:
            print(f"  ✗ Failed {csv.name}: {e}")
    
    print(f"\nDone! {len(csvs)} file(s) uploaded to Databricks.")
    print(f"The Auto Loader pipeline will ingest them automatically.")

if __name__ == "__main__":
    main()