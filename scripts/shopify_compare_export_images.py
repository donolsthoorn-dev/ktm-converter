#!/usr/bin/env python3
"""
Stap 1: lees shopify_export_all_*.csv, haal live productafbeeldingen op (alleen handles uit CSV),
vergelijk en rapporteer. Schrijf optioneel een takenbestand voor stap 2.

  python3 scripts/shopify_compare_export_images.py
  python3 scripts/shopify_compare_export_images.py --csv pad/naar/export.csv
  python3 scripts/shopify_compare_export_images.py --rest-only --workers 8
  python3 scripts/shopify_compare_export_images.py --no-tasks-file

Standaard: snelle GraphQL-batch + REST fallback. Stap 2: scripts/shopify_apply_missing_images.py
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
    build_tasks_payload,
    default_tasks_path,
    fetch_handle_maps_for_handles,
    latest_all_csv,
    norm_src,
    parse_csv_images,
    save_tasks_json,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Vergelijk Image Src in shopify_export_all-CSV met live Shopify (geen wijzigingen in de shop)."
    )
    ap.add_argument(
        "--csv",
        metavar="PATH",
        help=f"Pad naar shopify_export_all_*.csv (default: nieuwste in {config.PRODUCTS_OUTPUT_DIR})",
    )
    ap.add_argument(
        "--limit",
        type=int,
        metavar="N",
        default=0,
        help="Alleen eerste N handles uit de CSV (na sortering)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=8,
        metavar="N",
        help="Parallelle REST GET’s bij fallback (default: 8)",
    )
    ap.add_argument(
        "--fetch-workers",
        type=int,
        default=12,
        metavar="N",
        help="Parallelle GraphQL-batch requests (default: 12; niet bij --rest-only)",
    )
    ap.add_argument(
        "--graphql-batch",
        type=int,
        default=25,
        metavar="N",
        help="Handles per GraphQL-zoekquery (default: 25)",
    )
    ap.add_argument(
        "--rest-only",
        action="store_true",
        help="Alleen REST per handle (langzaam; zelfde als vroeger)",
    )
    ap.add_argument(
        "--tasks-out",
        metavar="PATH",
        default=None,
        help=f"JSON met ontbrekende images voor stap 2 (default: {default_tasks_path()})",
    )
    ap.add_argument(
        "--no-tasks-file",
        action="store_true",
        help="Geen JSON schrijven (alleen rapport in de terminal)",
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

    print(f"CSV: {csv_path}", flush=True)
    expected_by_handle = parse_csv_images(csv_path)
    if not expected_by_handle:
        print("Geen enkele rij met Image Src in deze CSV.", flush=True)
        return

    handles_sorted = sorted(expected_by_handle.keys())
    if args.limit and args.limit > 0:
        handles_sorted = handles_sorted[: args.limit]
        expected_by_handle = {h: expected_by_handle[h] for h in handles_sorted}

    print(
        f"Handles in CSV (met ten minste één image): {len(expected_by_handle)}",
        flush=True,
    )
    print("Live producten ophalen (alleen deze handles)...", flush=True)
    live_norms, live_id_by_handle = fetch_handle_maps_for_handles(
        handles_sorted,
        args.workers,
        graphql_batch=args.graphql_batch,
        fetch_workers=args.fetch_workers,
        rest_only=args.rest_only,
    )

    missing_report: list[tuple[str, str, list[str]]] = []
    not_in_shop: list[str] = []
    for handle in handles_sorted:
        urls = expected_by_handle[handle]
        if handle not in live_id_by_handle:
            not_in_shop.append(handle)
            continue
        have = live_norms.get(handle, set())
        missing = [u for u in urls if norm_src(u) not in have]
        if missing:
            missing_report.append((handle, live_id_by_handle[handle], missing))

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
        if not args.no_tasks_file:
            out_path = args.tasks_out if args.tasks_out is not None else default_tasks_path()
            payload = build_tasks_payload(csv_path, not_in_shop, [])
            save_tasks_json(out_path, payload)
            print(
                f"Takenbestand bijgewerkt (0 POST’s): {out_path}",
                flush=True,
            )
        return

    print(
        f"\nOntbrekende images: {len(missing_report)} producten, "
        f"{sum(len(x[2]) for x in missing_report)} URL-totalen.",
        flush=True,
    )
    for handle, pid, miss in missing_report[:30]:
        print(f"  {handle} (product id {pid or '?'}): {len(miss)} image(s)", flush=True)
        for u in miss[:3]:
            if len(u) > 100:
                print(f"    - {u[:100]}...", flush=True)
            else:
                print(f"    - {u}", flush=True)
        if len(miss) > 3:
            print(f"    ... {len(miss) - 3} meer", flush=True)
    if len(missing_report) > 30:
        print(f"  ... en {len(missing_report) - 30} producten meer", flush=True)

    if args.no_tasks_file:
        print(
            "\nKlaar (alleen rapport; geen JSON). Voor stap 2 opnieuw draaien zonder --no-tasks-file.",
            flush=True,
        )
        return

    out_path = args.tasks_out if args.tasks_out is not None else default_tasks_path()
    payload = build_tasks_payload(csv_path, not_in_shop, missing_report)
    save_tasks_json(out_path, payload)
    print(
        f"\nTakenbestand geschreven: {out_path}\n"
        f"Stap 2: python3 scripts/shopify_apply_missing_images.py --tasks {out_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
