#!/usr/bin/env python3
"""
Eén doorloop: delta-CSV ↔ live Shopify.

Per handle met minstens één echte foto in de CSV:
  - ontbrekende bestandsnaam toevoegen;
  - foto's waarvan de bestandsnaam niet meer in de CSV staat verwijderen;
  - de CSV-volgorde aanhouden, eerste foto vooraan.

Geen foto in de CSV voor een handle → galerij blijft staan.
Verwijderen gebeurt pas als de eerste CSV-foto op het product staat
(al aanwezig, of in deze run toegevoegd).

Voorbeelden (vanaf projectroot):

  python3 scripts/shopify_sync_missing_product_images.py --dry-run
  python3 scripts/shopify_sync_missing_product_images.py --csv output/products/shopify_export_delta_*.csv
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
    apply_gallery_cleanup,
    apply_missing_images_parallel,
    build_tasks_payload,
    compare_csv_gallery_against_live,
    default_tasks_path,
    flatten_tasks_from_payload,
    gallery_needs_reorder,
    gallery_replace_is_safe,
    latest_product_csv,
    save_tasks_json,
)


def _pretend_added(plan: dict) -> dict[str, str]:
    """Dry-run: doe alsof elke ontbrekende XML-foto een id krijgt."""
    keep = plan.get("keep_id_by_norm") or {}
    return {
        norm: f"new:{norm}"
        for norm in plan.get("expected_norms") or []
        if norm not in keep
    }


def _gallery_counts(plans: dict, added: dict[str, str] | None = None) -> tuple[int, int]:
    """(te verwijderen foto's, te herschikken producten) gegeven geslaagde adds."""
    delete_n = 0
    reorder_n = 0
    for handle, plan in plans.items():
        if added is None:
            new_ids = _pretend_added(plan)
        else:
            new_ids = {
                norm: image_id
                for (h, norm), image_id in added.items()
                if h == handle and image_id
            }
        if not gallery_replace_is_safe(plan, new_ids):
            continue
        delete_n += len(plan.get("delete_ids") or [])
        if gallery_needs_reorder(plan, new_ids):
            reorder_n += 1
    return delete_n, reorder_n


def _print_stale_summary(plans: dict) -> None:
    stale = [
        (handle, plan)
        for handle, plan in plans.items()
        if plan.get("delete_ids")
    ]
    if not stale:
        print("\nGeen foto's die niet meer in de CSV staan.", flush=True)
        return
    total = sum(len(plan.get("delete_ids") or []) for _h, plan in stale)
    print(
        f"\nFoto's niet meer in de CSV: {len(stale)} producten, {total} foto's.",
        flush=True,
    )
    for handle, plan in stale[:25]:
        print(
            f"  {handle} (product id {plan.get('product_id') or '?'}): "
            f"{len(plan.get('delete_ids') or [])} oude foto('s)",
            flush=True,
        )
    if len(stale) > 25:
        print(f"  ... en {len(stale) - 25} producten meer", flush=True)


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
            "Vergelijk delta-CSV met live Shopify: ontbrekende foto's toevoegen "
            "en foto's die niet meer in de CSV staan verwijderen."
        )
    )
    ap.add_argument(
        "--csv",
        metavar="PATH",
        help=f"Product-CSV (default: nieuwste shopify_export_delta_*.csv in {config.PRODUCTS_OUTPUT_DIR})",
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

    csv_path = args.csv or latest_product_csv(config.PRODUCTS_OUTPUT_DIR)
    if not csv_path or not os.path.isfile(csv_path):
        print(
            "Geen CSV: geef --csv of zet shopify_export_delta_*.csv in output/products/.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    tasks_out = args.tasks_out if args.tasks_out is not None else default_tasks_path()

    print(f"CSV: {csv_path}", flush=True)
    print("Stap 1/3: CSV ↔ live Shopify vergelijken...", flush=True)
    missing_report, not_in_shop, expected_by_handle, plans = compare_csv_gallery_against_live(
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
    payload["replacements"] = [
        {
            "handle": handle,
            "product_id": plan.get("product_id"),
            "delete_ids": plan.get("delete_ids") or [],
            "missing_urls": plan.get("missing_urls") or [],
        }
        for handle, plan in plans.items()
        if plan.get("delete_ids") or plan.get("missing_urls")
    ]
    save_tasks_json(tasks_out, payload)
    print(f"Takenbestand: {tasks_out}", flush=True)

    _print_missing_summary(missing_report, not_in_shop)
    _print_stale_summary(plans)

    if args.compare_only:
        print("\nKlaar (--compare-only: geen POST’s).", flush=True)
        return

    tasks = flatten_tasks_from_payload(payload)
    n = len(tasks)
    would_delete, would_reorder = _gallery_counts(plans)
    if n == 0 and would_delete == 0 and would_reorder == 0:
        print("\nGalerij komt al overeen met de CSV.", flush=True)
        print(
            "DIGEST_METRIC missing_images_ok=0 missing_images_fail=0 "
            "images_replaced=0 images_replaced_fail=0",
            flush=True,
        )
        return

    if args.dry_run:
        print(
            f"\nDry-run: zou {n} image-POST’s uitvoeren, "
            f"{would_delete} oude foto's verwijderen, "
            f"{would_reorder} galerijen herschikken (geen wijzigingen).",
            flush=True,
        )
        return

    prefer_input = not args.skip_input
    added_ids: dict[tuple[str, str], str] = {}
    ok = fail = 0
    if n:
        print(
            f"\nStap 2/3: {n} image(s) koppelen "
            f"(apply-workers={args.apply_workers}, "
            f"{'lokale input + ensure_image waar mogelijk' if prefer_input else 'alleen CSV-URL’s'})...",
            flush=True,
        )
        ok, fail = apply_missing_images_parallel(
            tasks,
            args.apply_workers,
            input_dir=args.input_dir,
            prefer_input=prefer_input,
            added_ids=added_ids,
        )
    else:
        print("\nStap 2/3: geen nieuwe foto's om toe te voegen.", flush=True)

    print(
        f"\nStap 3/3: oude foto's verwijderen en XML-volgorde zetten "
        f"({len(plans)} producten met een afwijkende galerij)...",
        flush=True,
    )
    deleted_ok, deleted_fail, reordered = apply_gallery_cleanup(
        plans,
        added_ids,
        args.apply_workers,
    )
    print(
        f"\nKlaar: toegevoegd OK={ok}, mislukt={fail}, "
        f"verwijderd={deleted_ok}, verwijderen mislukt={deleted_fail}, "
        f"herschikt={reordered}.",
        flush=True,
    )
    print(
        f"DIGEST_METRIC missing_images_ok={ok} missing_images_fail={fail} "
        f"images_replaced={deleted_ok} images_replaced_fail={deleted_fail}",
        flush=True,
    )


if __name__ == "__main__":
    main()
