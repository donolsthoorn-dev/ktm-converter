#!/usr/bin/env python3
"""
Ververs Motox Shopify SKU/handle/product-id caches onder cache/motox/.

  python3 scripts/motox_ebihr_refresh_cache.py
  python3 scripts/motox_ebihr_refresh_cache.py --force

Laadt credentials uit .env.motox.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.motox_shopify_cache import (  # noqa: E402
    load_motox_indexes,
    refresh_motox_cache,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Ververs Motox Shopify-caches (cache/motox/).")
    ap.add_argument(
        "--force",
        action="store_true",
        default=True,
        help="Altijd opnieuw ophalen (default: ja).",
    )
    ap.add_argument(
        "--no-force",
        action="store_true",
        help="Alleen ophalen als cache ontbreekt.",
    )
    ap.add_argument(
        "--status",
        action="store_true",
        help="Toon huidige cache-status zonder netwerk.",
    )
    args = ap.parse_args()

    if args.status:
        idx = load_motox_indexes()
        s = idx["summary"]
        print(
            f"Motox-cache present={s['present']} products={s['products']} "
            f"skus={s['skus']} dir={s['cache_dir']}"
        )
        return 0

    force = not args.no_force
    summary = refresh_motox_cache(force=force)
    print(
        f"Klaar: products={summary.get('products')} skus={summary.get('skus')} "
        f"→ {summary.get('cache_dir')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
