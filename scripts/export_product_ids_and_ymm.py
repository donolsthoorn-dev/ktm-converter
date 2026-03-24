#!/usr/bin/env python3
"""Generate Product-Ids-style CSV + YMM fitment CSV from KTM XML."""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(
        description="Product-Ids + YMM CSV uit KTM XML (Shopify-cache: zie docs/shopify_cache_en_scheduling.md).",
    )
    parser.add_argument(
        "--refresh-shopify-cache",
        action="store_true",
        help="Shopify-cache opnieuw ophalen (zelfde als KTM_FORCE_REFRESH_SHOPIFY_CACHE=1).",
    )
    args = parser.parse_args()
    if args.refresh_shopify_cache:
        os.environ["KTM_FORCE_REFRESH_SHOPIFY_CACHE"] = "1"

    from modules.ymm_export import run_exports

    print("Start export (werkmap:", os.getcwd(), ")…", flush=True)
    p1, p2, n = run_exports()
    print("Product-Ids template:", p1)
    print("YMM app import (all rows):", p2, f"({n} data rows)")


if __name__ == "__main__":
    main()
