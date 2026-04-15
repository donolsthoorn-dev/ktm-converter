#!/usr/bin/env python3
"""
Eenmalig: bouw shopify_pricelist_sync_state.json vanuit een **KTM prijs-CSV** (ERP-export) — **zonder** API.

Wil je de basis uit een **Shopify product-export**? Gebruik dan:
  scripts/bootstrap_state_from_shopify_export.py

Zelfde berekening als shopify_sync_from_pricelist_csv.py (ETA, prijs incl. BTW, draft bij status 80).

Gebruik dit alleen als de **werkelijke** prijzen/ETA/status in Shopify **al gelijk zijn** aan dit
bestand (bijv. na een import of als je bewust geen massale eerste sync wilt). Anders liegen we in
de state en worden echte verschillen niet meer geüpload.

Workflow (eerste keer “rustig”):
  1) python3 scripts/shopify_refresh_variant_cache.py
  2) python3 scripts/bootstrap_pricelist_sync_state_from_csv.py --csv input/jouw_export.csv
  3) python3 scripts/shopify_sync_from_pricelist_csv.py --csv input/jouw_export.csv
     → alleen echte wijzigingen t.o.v. dit bestand

Opties:
  (default) alleen SKU's die in de variant-cache zitten
  --all-csv-skus           alle SKU-regels uit de CSV in state (ook zonder Shopify-variant)

Maak eerst een backup van je huidige state als die al data bevat:
  cp cache/shopify_pricelist_sync_state.json cache/shopify_pricelist_sync_state.json.bak
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_sync_module():
    path = PROJECT_ROOT / "scripts" / "shopify_sync_from_pricelist_csv.py"
    spec = importlib.util.spec_from_file_location("shopify_sync_from_pricelist_csv", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Kan module niet laden: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    sync = _load_sync_module()
    sync.load_dotenv()

    p = argparse.ArgumentParser(
        description="Vul shopify_pricelist_sync_state.json vanuit KTM prijs-CSV (geen API)"
    )
    p.add_argument(
        "--csv",
        metavar="PAD",
        help="KTM prijs-CSV (default: eerste bekende merk-export of *_Z1_EUR_EN_csv.csv in input/)",
    )
    p.add_argument(
        "--state-file",
        type=Path,
        default=sync.DEFAULT_STATE_FILE,
        help=f"Uitvoer (default: {sync.DEFAULT_STATE_FILE})",
    )
    p.add_argument(
        "--variant-cache",
        type=Path,
        default=sync.DEFAULT_VARIANT_CACHE,
        help="Voor --only-in-variant-cache",
    )
    p.add_argument(
        "--all-csv-skus",
        action="store_true",
        help="Zet state voor alle SKU's uit CSV (default: alleen SKU's die in variant-cache staan)",
    )
    args = p.parse_args()

    only_cache = not args.all_csv_skus

    csv_path = sync.resolve_csv_path(args.csv)
    today = date.today()
    desired = sync.read_pricelist_csv_desired(csv_path, today)

    if only_cache:
        cache_path = args.variant_cache.resolve()
        if not cache_path.is_file():
            print(
                f"Variant-cache ontbreekt: {cache_path}\n"
                "  python3 scripts/shopify_refresh_variant_cache.py\n"
                "Of gebruik --all-csv-skus.",
                flush=True,
            )
            return 1
        sku_to_vp = sync.load_variant_cache(cache_path)
        state: dict = {}
        skipped = 0
        for sku, d in desired.items():
            if sku not in sku_to_vp:
                skipped += 1
                continue
            state[sku] = _state_entry_from_desired(d)
        print(
            f"CSV: {csv_path}\n"
            f"  Regels in CSV (unieke SKU): {len(desired)}\n"
            f"  In state gezet (ook in variant-cache): {len(state)}\n"
            f"  Overgeslagen (niet in Shopify-cache): {skipped}",
            flush=True,
        )
    else:
        state = {sku: _state_entry_from_desired(d) for sku, d in desired.items()}
        print(
            f"CSV: {csv_path}\n"
            f"  Alle {len(state)} SKU's in state gezet (--all-csv-skus).",
            flush=True,
        )

    out = args.state_file.resolve()
    sync.save_state(out, state)
    print(f"Geschreven: {out}", flush=True)
    print(
        "Controleer of Shopify met deze waarden overeenkomt; zo niet, sync niet overslaan "
        "of state aanpassen.",
        flush=True,
    )
    return 0


def _state_entry_from_desired(d: dict) -> dict:
    """Zelfde velden als shopify_sync_from_pricelist_csv na succesvolle sync."""
    entry: dict = {
        "eta_iso": d["eta_iso"],
        "product_status": d["product_status"],
    }
    if d.get("price_incl") is not None:
        entry["price_incl"] = d["price_incl"]
    return entry


if __name__ == "__main__":
    raise SystemExit(main())
