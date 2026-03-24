#!/usr/bin/env python3
"""
Build a Metafields Manager–style product CSV (fits_on JSON + flat YMM columns).

Prerequisite: run scripts/export_product_ids_and_ymm.py first so
  output/reports/product_ids_from_xml.csv
exists with Product Id + handle mapping (or pass --product-ids).

Output default:
  output/reports/product_metafields_metafields_manager.csv
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.metafields_manager_export import run_metafields_export  # noqa: E402


def main():
    p = argparse.ArgumentParser(
        description="Metafields Manager product CSV uit KTM XML + product_ids_from_xml.",
    )
    p.add_argument(
        "--product-ids",
        default=None,
        help="Pad naar product_ids_from_xml.csv (default: output/reports/...).",
    )
    p.add_argument(
        "-o",
        "--output",
        default=None,
        help="Uitvoer-CSV (default: output/reports/product_metafields_metafields_manager.csv).",
    )
    p.add_argument(
        "--merge-from-shopify-csv",
        default=None,
        metavar="PATH",
        help="Shopify product-export (CSV) met kolom Handle + fits_on/Fits on: vult ontbrekende "
        "fits_on en voegt producten toe die niet in de KTM-XML staan.",
    )
    args = p.parse_args()

    out, n = run_metafields_export(
        product_ids_path=args.product_ids,
        output_path=args.output,
        shopify_merge_csv=args.merge_from_shopify_csv,
    )
    print("Metafields Manager CSV:", out, f"({n} regels; zie console voor aantal mét fits_on)", flush=True)


if __name__ == "__main__":
    main()
