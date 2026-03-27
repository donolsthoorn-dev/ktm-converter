#!/usr/bin/env python3
"""
Zoek één SKU op: staat die in de all- en delta-Shopify-export (zelfde logica als main.py / exporter),
en zo niet, waarom niet (zelfde teksten als shopify_export_excluded_*.csv).

Voorbeeld (vanaf projectroot):

  python3 scripts/sku_export_status.py a62612995001

Standaard: geen netwerk — alleen image_cache.json + lokale bestanden (snel). Voor CDN/Shopify-lookup
zoals main.py bij lege cache:

  python3 scripts/sku_export_status.py A62612995001 --network
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402, F401 — laadt .env

from modules.sku_probe import analyze_sku  # noqa: E402

VARIANT_SKU_COL = "Variant SKU"


def _latest_csv(pattern: str, directory: str) -> str | None:
    paths = glob.glob(os.path.join(directory, pattern))
    if not paths:
        return None
    return max(paths, key=os.path.getmtime)


def _sku_in_csv_file(path: str, sku: str) -> bool:
    q = sku.strip().upper()
    try:
        with open(path, encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            if not r.fieldnames or VARIANT_SKU_COL not in r.fieldnames:
                return False
            for row in r:
                cell = (row.get(VARIANT_SKU_COL) or "").strip().upper()
                if cell == q:
                    return True
    except OSError:
        return False
    return False


def main() -> int:
    p = argparse.ArgumentParser(description="SKU-status t.o.v. all/delta Shopify-export")
    p.add_argument("sku", help="SKU, bijv. a62612995001")
    p.add_argument(
        "--network",
        action="store_true",
        help="Afbeeldingen: CDN-HEAD + Shopify file-lookup (langzaam; dichter bij main.py als cache leeg is)",
    )
    p.add_argument(
        "--products-dir",
        default=config.PRODUCTS_OUTPUT_DIR,
        help=f"Map met shopify_export_*.csv (default: {config.PRODUCTS_OUTPUT_DIR})",
    )
    args = p.parse_args()

    result = analyze_sku(args.sku, use_network=args.network)

    if not result.get("found"):
        print(f"SKU niet gevonden in XML-catalogus: {result.get('sku_query', args.sku)!r}")
        print("(Controleer spelling, of dat de XML/0150-input actueel is.)")
        return 1

    sku = result["sku"]
    print(f"SKU:          {sku}")
    print(f"Handle:       {result['handle']}")
    if result.get("title"):
        print(f"Titel:        {result['title'][:120]}{'…' if len(result.get('title', '')) > 120 else ''}")
    print(f"Varianttype:  {result['type'] or '(leeg)'}")
    print(f"Primair type: {result['primary_type'] or '(leeg)'} (langste titel in productgroep)")
    print()

    in_all = result["in_all_csv"]
    in_delta = result["in_delta_csv"]
    print("Berekend (zelfde regels als main.py + exporter.py):")
    print(f"  In shopify_export ALL:   {'ja' if in_all else 'nee'}")
    print(f"  In shopify_export DELTA: {'ja' if in_delta else 'nee'}")
    reden = (result.get("reden") or "").strip()
    if reden:
        print(f"  Reden (delta / CSV):     {reden}")
    else:
        print("  Reden (delta / CSV):     — (variant komt in beide exportlijsten voor zover filters)")
    print()

    all_path = _latest_csv("shopify_export_all_*.csv", args.products_dir)
    delta_path = _latest_csv("shopify_export_delta_*.csv", args.products_dir)

    print(f"Controle op schijf ({args.products_dir}):")
    if all_path:
        ok = _sku_in_csv_file(all_path, sku)
        print(f"  Laatste ALL:   {os.path.basename(all_path)}  →  SKU {'gevonden' if ok else 'niet gevonden'}")
    else:
        print("  Laatste ALL:   (geen shopify_export_all_*.csv)")
    if delta_path:
        ok = _sku_in_csv_file(delta_path, sku)
        print(f"  Laatste DELTA: {os.path.basename(delta_path)}  →  SKU {'gevonden' if ok else 'niet gevonden'}")
    else:
        print("  Laatste DELTA: (geen shopify_export_delta_*.csv)")
    if not args.network:
        print()
        print(
            "Let op: zonder --network kan ‘geen afbeeldingen’ strenger zijn dan main.py "
            "als CDN/Shopify nog niet in image_cache.json staat."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
