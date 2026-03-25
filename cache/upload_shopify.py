import csv
import os
import sys
from glob import glob
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402

SHOPIFY_STORE = config.SHOPIFY_SHOP_SLUG
SHOPIFY_TOKEN = config.SHOPIFY_ACCESS_TOKEN

API_VERSION = config.SHOPIFY_ADMIN_API_VERSION


def find_latest_csv():

    files = glob(os.path.join(config.PRODUCTS_OUTPUT_DIR, "shopify_export_*.csv"))

    if not files:
        raise Exception("Geen Shopify export CSV gevonden")

    latest = max(files, key=os.path.getctime)

    print(f"Laatste CSV gevonden:\n{latest}")

    return latest


def create_test_csv(source_csv, limit=5):

    test_csv = os.path.join(config.PRODUCTS_OUTPUT_DIR, "test_upload.csv")

    handles_seen = set()
    rows_out = []

    with open(source_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            handle = row.get("URL handle", "").strip()

            if handle not in handles_seen:
                if len(handles_seen) >= limit:
                    break

                handles_seen.add(handle)

            rows_out.append(row)

    with open(test_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Test CSV gemaakt met {len(handles_seen)} producten")

    return test_csv


def upload_csv(file_path):

    url = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{API_VERSION}/products/import.json"

    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}

    with open(file_path, "rb") as f:
        files = {"file": f}

        print("CSV uploaden naar Shopify...")

        r = requests.post(url, headers=headers, files=files)

    print("Status:", r.status_code)

    try:
        print(r.json())
    except Exception:
        print(r.text)


if __name__ == "__main__":
    source_csv = find_latest_csv()

    upload_csv(source_csv)
