#!/usr/bin/env python3
"""
Eén doorloop: shopify_export_all-CSV ↔ live Shopify → ontbrekende productafbeeldingen
toevoegen (zelfde logica als shopify_compare_export_images + shopify_apply_missing_images).

Per ontbrekende URL (t.o.v. CSV, genormaliseerde bestandsnaam):
  - Zonder --skip-input: eerst lokaal bestand onder input/ matchen → ensure_image
    (cache / CDN / Shopify Files-lookup / upload), daarna REST POST naar het product.
  - Met --skip-input: POST met de CSV-URL (Shopify haalt zelf op).

Voorbeelden (vanaf projectroot):

  python3 scripts/shopify_sync_missing_product_images.py --dry-run
  python3 scripts/shopify_sync_missing_product_images.py --csv output/products/shopify_export_all_*.csv
  python3 scripts/shopify_sync_missing_product_images.py --compare-only
  python3 scripts/shopify_sync_missing_product_images.py --apply-workers 12 --limit 100

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN; geen KTM_SKIP_SHOPIFY_API=1.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

from modules.shopify_export_images_lib import (  # noqa: E402
    apply_missing_images_parallel,
    build_tasks_payload,
    compare_csv_missing_images_against_live,
    default_tasks_path,
    flatten_tasks_from_payload,
    latest_all_csv,
    load_tasks_json,
    save_tasks_json,
)


def _print_missing_summary(missing_report: list, not_in_shop: list) -> None:
    if not_in_shop:
        print(
            f"\nHandles in CSV maar niet gevonden in live productlijst ({len(not_in_shop)}):",
            flush=True,
        )
        for h in not_in_shop[:50]:
            print(f"  {h}", flush=True)
        if len(not_in_shop) > 50:
            print(f"  ... en {len(not_in_shop) - 50} meer", flush=True)

    if not missing_report:
        print("\nGeen ontbrekende images t.o.v. CSV (voor verwerkte handles).", flush=True)
        return

    print(
        f"\nOntbrekende images: {len(missing_report)} producten, "
        f"{sum(len(x[2]) for x in missing_report)} URL-totalen.",
        flush=True,
    )
    for handle, pid, miss in missing_report[:25]:
        print(f"  {handle} (product id {pid or '?'}): {len(miss)} image(s)", flush=True)
        for u in miss[:2]:
            u = str(u)
            print(f"    - {u[:110]}..." if len(u) > 110 else f"    - {u}", flush=True)
        if len(miss) > 2:
            print(f"    ... {len(miss) - 2} meer", flush=True)
    if len(missing_report) > 25:
        print(f"  ... en {len(missing_report) - 25} producten meer", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Vergelijk ALL-product-CSV met live Shopify en koppel ontbrekende afbeeldingen "
            "(compare + apply in één script)."
        )
    )
    ap.add_argument(
        "--csv",
        metavar="PATH",
        help=f"shopify_export_all_*.csv (default: nieuwste in {config.PRODUCTS_OUTPUT_DIR})",
    )
    ap.add_argument(
        "--tasks-out",
        metavar="PATH",
        default=None,
        help=f"JSON-taken (default: {default_tasks_path()})",
    )
    ap.add_argument(
        "--limit",
        type=int,
        metavar="N",
        default=0,
        help="Alleen eerste N handles uit de CSV (na sortering); 0 = alle",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=8,
        metavar="N",
        help="REST workers bij GraphQL-fallback (default: 8)",
    )
    ap.add_argument(
        "--fetch-workers",
        type=int,
        default=12,
        metavar="N",
        help="Parallelle GraphQL-batch requests (default: 12)",
    )
    ap.add_argument(
        "--graphql-batch",
        type=int,
        default=25,
        metavar="N",
        help="Handles per GraphQL-query (default: 25)",
    )
    ap.add_argument(
        "--rest-only",
        action="store_true",
        help="Alleen REST per handle (langzaam)",
    )
    ap.add_argument(
        "--compare-only",
        action="store_true",
        help="Alleen vergelijken + JSON schrijven, geen POST’s",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Geen POST’s; wel compare + JSON + telling",
    )
    ap.add_argument(
        "--apply-workers",
        type=int,
        default=8,
        metavar="N",
        help="Parallelle product-image POST’s (default: 8)",
    )
    ap.add_argument(
        "--skip-input",
        action="store_true",
        help="Geen input/ + ensure_image; alleen CSV-URL’s naar POST",
    )
    ap.add_argument(
        "--input-dir",
        metavar="DIR",
        default=None,
        help=f"Lokale image-zoekmap (default: {config.INPUT_DIR})",
    )
    args = ap.parse_args()

    if os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        print(
            "KTM_SKIP_SHOPIFY_API is gezet — dit script heeft live Shopify nodig.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not config.SHOPIFY_ACCESS_TOKEN:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", file=sys.stderr)
        raise SystemExit(1)

    csv_path = args.csv or latest_all_csv(config.PRODUCTS_OUTPUT_DIR)
    if not csv_path or not os.path.isfile(csv_path):
        print(
            "Geen CSV: geef --csv of zet shopify_export_all_*.csv in output/products/.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    tasks_out = args.tasks_out if args.tasks_out is not None else default_tasks_path()

    print(f"CSV: {csv_path}", flush=True)
    print("Stap 1/2: CSV ↔ live Shopify vergelijken...", flush=True)
    missing_report, not_in_shop, expected_by_handle = compare_csv_missing_images_against_live(
        csv_path,
        limit=args.limit or 0,
        rest_workers=args.workers,
        graphql_batch=args.graphql_batch,
        fetch_workers=args.fetch_workers,
        rest_only=args.rest_only,
    )
    if not expected_by_handle:
        print("Geen enkele rij met Image Src in deze CSV.", flush=True)
        return

    payload = build_tasks_payload(csv_path, not_in_shop, missing_report)
    save_tasks_json(tasks_out, payload)
    print(f"Takenbestand: {tasks_out}", flush=True)

    _print_missing_summary(missing_report, not_in_shop)

    if args.compare_only:
        print("\nKlaar (--compare-only: geen POST’s).", flush=True)
        return

    tasks = flatten_tasks_from_payload(load_tasks_json(tasks_out))
    n = len(tasks)
    if n == 0:
        print("\nGeen image-POST’s nodig.", flush=True)
        return

    if args.dry_run:
        print(f"\nDry-run: zou {n} image-POST’s uitvoeren (geen wijzigingen).", flush=True)
        return

    prefer_input = not args.skip_input
    print(
        f"\nStap 2/2: {n} image(s) koppelen "
        f"(apply-workers={args.apply_workers}, "
        f"{'lokale input + ensure_image waar mogelijk' if prefer_input else 'alleen CSV-URL’s'})...",
        flush=True,
    )
    ok, fail = apply_missing_images_parallel(
        tasks,
        args.apply_workers,
        input_dir=args.input_dir,
        prefer_input=prefer_input,
    )
    print(f"\nKlaar: OK={ok}, mislukt={fail}.", flush=True)


if __name__ == "__main__":
    main()
