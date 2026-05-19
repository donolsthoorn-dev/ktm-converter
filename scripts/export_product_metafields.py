#!/usr/bin/env python3
"""
Build a Metafields Manager–style product CSV (fits_on JSON + flat YMM columns).

Prerequisite: run scripts/export_product_ids_and_ymm.py first so
  output/<merk>/ids/product_ids_from_xml.csv
exists with Product Id + handle mapping (or pass --product-ids).

Output default:
  output/<merk>/metafields/product_metafields_metafields_manager.csv
  (of …_delta.csv bij --delta-handles-csv / --delta-handles-file)
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.brand_cli import (  # noqa: E402
    add_brand_argument,
    apply_parsed_brand,
    bootstrap_brand_from_argv,
)

bootstrap_brand_from_argv()

import config  # noqa: E402, F401 — laadt .env
from modules.metafields_manager_export import run_metafields_export  # noqa: E402


def main():
    p = argparse.ArgumentParser(
        description=(
            "Metafields Manager product CSV uit merk-XML + product_ids_from_xml "
            f"(default onder {config.METAFIELDS_OUTPUT_DIR})."
        ),
    )
    add_brand_argument(p)
    p.add_argument(
        "--product-ids",
        default=None,
        help=f"Pad naar product_ids_from_xml.csv (default: {config.IDS_OUTPUT_DIR}/…).",
    )
    p.add_argument(
        "-o",
        "--output",
        default=None,
        help=(
            "Uitvoer-CSV "
            f"(default: {config.METAFIELDS_OUTPUT_DIR}/product_metafields_metafields_manager.csv)."
        ),
    )
    p.add_argument(
        "--merge-from-shopify-csv",
        default=None,
        metavar="PATH",
        help="Shopify product-export (CSV) met kolom Handle + fits_on/Fits on: vult ontbrekende "
        "fits_on en voegt producten toe die niet in de merk-XML staan.",
    )
    dh = p.add_mutually_exclusive_group()
    dh.add_argument(
        "--delta-handles-csv",
        metavar="PATH",
        help="Alleen regels voor handles uit deze CSV (kolom Handle).",
    )
    dh.add_argument(
        "--delta-handles-file",
        metavar="PATH",
        help="Eén handle per regel (# = commentaar).",
    )
    p.add_argument(
        "--ymm-makes",
        metavar="MAKE",
        nargs="+",
        help="Zelfde Make-filter als YMM-export (default: KTM Husqvarna GASGAS).",
    )
    p.add_argument(
        "--ymm-all-makes",
        action="store_true",
        help="Geen Make-filter op fits_on (alle merken uit XML).",
    )
    args = p.parse_args()
    apply_parsed_brand(args.brand)

    filter_handles = None
    if args.delta_handles_csv:
        from modules.delta_handles import load_handles_from_shopify_export_csv

        filter_handles = load_handles_from_shopify_export_csv(args.delta_handles_csv)
    elif args.delta_handles_file:
        from modules.delta_handles import load_handles_from_text_file

        filter_handles = load_handles_from_text_file(args.delta_handles_file)

    from modules.ymm_export import resolve_ymm_make_filter

    filter_makes = resolve_ymm_make_filter(
        args.ymm_makes,
        all_makes=args.ymm_all_makes,
    )

    out, n = run_metafields_export(
        product_ids_path=args.product_ids,
        output_path=args.output,
        shopify_merge_csv=args.merge_from_shopify_csv,
        filter_handles=filter_handles,
        filter_makes=filter_makes,
        ymm_all_makes=args.ymm_all_makes,
    )
    print(
        f"Metafields Manager CSV (merk={config.BRAND_ID}):",
        out,
        f"({n} productregels; zie console voor split-delen en aantal mét fits_on)",
        flush=True,
    )


if __name__ == "__main__":
    main()
