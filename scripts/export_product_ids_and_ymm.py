#!/usr/bin/env python3
"""Generate Product-Ids-style CSV + YMM fitment CSV from brand XML (KTM / HSQ / WP)."""

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

import config  # noqa: E402, F401 — laadt .env + merk-paden


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Product-Ids + YMM CSV uit merk-XML "
            f"(output onder {config.BASE_OUTPUT_DIR}/ids en …/ymm). "
            "Shopify-cache: docs/shopify_cache_en_scheduling.md."
        ),
    )
    add_brand_argument(parser)
    parser.add_argument(
        "--refresh-shopify-cache",
        action="store_true",
        help="Shopify-cache opnieuw ophalen (zelfde als KTM_FORCE_REFRESH_SHOPIFY_CACHE=1).",
    )
    h = parser.add_mutually_exclusive_group()
    h.add_argument(
        "--delta-handles-csv",
        metavar="PATH",
        help=(
            "Alleen deze producthandles (kolom Handle), bijv. "
            f"{config.PRODUCTS_OUTPUT_DIR}/shopify_export_delta_….csv"
        ),
    )
    h.add_argument(
        "--delta-handles-file",
        metavar="PATH",
        help="Tekstbestand: één handle per regel (# = commentaar).",
    )
    parser.add_argument(
        "--ymm-makes",
        metavar="MAKE",
        nargs="+",
        help=(
            "Alleen deze Make-waarden in YMM (bv. KTM Husqvarna GASGAS). "
            "Standaard: KTM + Husqvarna + GASGAS."
        ),
    )
    parser.add_argument(
        "--ymm-all-makes",
        action="store_true",
        help="Geen Make-filter (ook Yamaha, Kawasaki, Ducati, …).",
    )
    args = parser.parse_args()
    apply_parsed_brand(args.brand)

    if args.refresh_shopify_cache:
        os.environ["KTM_FORCE_REFRESH_SHOPIFY_CACHE"] = "1"

    filter_handles = None
    if args.delta_handles_csv:
        from modules.delta_handles import load_handles_from_shopify_export_csv

        filter_handles = load_handles_from_shopify_export_csv(args.delta_handles_csv)
    elif args.delta_handles_file:
        from modules.delta_handles import load_handles_from_text_file

        filter_handles = load_handles_from_text_file(args.delta_handles_file)

    from modules.ymm_export import resolve_ymm_make_filter, run_exports

    filter_makes = resolve_ymm_make_filter(
        args.ymm_makes,
        all_makes=args.ymm_all_makes,
    )

    print(
        f"Start export merk={config.BRAND_ID} (werkmap: {os.getcwd()})…",
        flush=True,
    )
    print(f"  XML: {config.XML_FILE}", flush=True)
    p1, p2, n = run_exports(
        filter_handles=filter_handles,
        filter_makes=filter_makes,
        ymm_all_makes=args.ymm_all_makes,
    )
    print("Product-Ids template:", p1)
    print("YMM app import:", p2, f"({n} data rows)")


if __name__ == "__main__":
    main()
