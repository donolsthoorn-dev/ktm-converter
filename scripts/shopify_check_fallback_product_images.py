#!/usr/bin/env python3
"""
Producten in Shopify met placeholder-afbeelding (pho_fallback_fallback_no_picture…)
controleren op nieuw beschikbare bronnen.

Stappen:
  1. Lees shopify_export_all_*.csv: handles met fallback-URL in de export.
  2. Haal live productmedia op (GraphQL + REST fallback).
  3. Filter: live heeft alleen fallback (geen echte foto op het product).
  4. Per handle: echte URL's in CSV? Lokale XML-referenties + cache/CDN (zonder upload)?

Uitvoer: CSV op stdout (kolommen met ;), samenvatting op stderr.

Voorbeelden (vanaf projectroot):

  python3 scripts/shopify_check_fallback_product_images.py > output/logs/fallback_image_check.csv
  python3 scripts/shopify_check_fallback_product_images.py --brand hsq --limit 200
  python3 scripts/shopify_check_fallback_product_images.py --no-xml-check

Koppelen na bevindingen (bestaande flow):
  python3 scripts/shopify_compare_export_images.py
  python3 scripts/shopify_apply_missing_images.py

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN; geen KTM_SKIP_SHOPIFY_API=1.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

from modules.image_manager import (  # noqa: E402
    load_cache,
    resolve_image_url_without_upload,
)
from modules.image_resolve import build_basename_index, resolve_local_image  # noqa: E402
from modules.shopify_export_images_lib import (  # noqa: E402
    fetch_handle_maps_for_handles,
    is_fallback_image_url,
    latest_all_csv,
    live_norms_are_fallback_only,
    norm_src,
    parse_csv_images_split,
)
from modules.xml_loader import load_products, normalize_shopify_product_handle  # noqa: E402


def _norm_ref_key(s: str) -> str:
    return s.strip().replace("\\", "/").lower()


def _first_sku_from_csv_rows(path: str) -> dict[str, str]:
    """Handle -> eerste niet-lege Variant SKU in de export."""
    out: dict[str, str] = {}
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return out
        for row in reader:
            h = normalize_shopify_product_handle(row.get("Handle") or "")
            if not h or h in out:
                continue
            sku = (row.get("Variant SKU") or "").strip()
            if sku:
                out[h] = sku
    return out


def _build_xml_image_index(
    products: list[dict],
) -> dict[str, list[str]]:
    """Genormaliseerde handle -> unieke XML image-referenties."""
    by_handle: dict[str, list[str]] = defaultdict(list)
    seen: dict[str, set[str]] = defaultdict(set)
    for p in products:
        h = normalize_shopify_product_handle(p.get("handle") or "")
        if not h:
            continue
        for img in p.get("images") or []:
            raw = (img or "").strip()
            if not raw:
                continue
            k = _norm_ref_key(raw)
            if k in seen[h]:
                continue
            seen[h].add(k)
            by_handle[h].append(raw)
    return dict(by_handle)


def _resolve_xml_images_for_handle(
    refs: list[str],
    input_root: Path,
    by_exact: dict[str, list[Path]],
    by_lower: dict[str, list[Path]],
    cache: dict,
    *,
    use_network: bool,
) -> list[str]:
    """Publieke URL's (geen fallback) die nu beschikbaar zijn zonder upload."""
    urls: list[str] = []
    seen_norm: set[str] = set()
    for ref in refs:
        local_path = resolve_local_image(ref, input_root, by_exact, by_lower)
        if local_path is None:
            continue
        url = resolve_image_url_without_upload(
            local_path.name,
            local_path,
            cache,
            use_network=use_network,
        )
        if not url or is_fallback_image_url(url):
            continue
        n = norm_src(url)
        if not n or n in seen_norm:
            continue
        seen_norm.add(n)
        urls.append(url)
    return urls


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Vind Shopify-producten met alleen fallback-afbeelding live en "
            "rapporteer of er inmiddels echte bronnen zijn (CSV / XML+input)."
        )
    )
    ap.add_argument(
        "--csv",
        metavar="PATH",
        help=f"shopify_export_all_*.csv (default: nieuwste in {config.PRODUCTS_OUTPUT_DIR})",
    )
    ap.add_argument(
        "--brand",
        metavar="ID",
        default=None,
        help="Merk (ktm, hsq, wp); herlaadt paden via config.apply_brand",
    )
    ap.add_argument(
        "--limit",
        type=int,
        metavar="N",
        default=0,
        help="Maximaal N fallback-only handles controleren (0 = alle)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=8,
        metavar="N",
        help="REST workers (default: 8)",
    )
    ap.add_argument(
        "--fetch-workers",
        type=int,
        default=12,
        metavar="N",
        help="GraphQL batch workers (default: 12)",
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
        help="Alleen REST per handle",
    )
    ap.add_argument(
        "--no-xml-check",
        action="store_true",
        help="Geen XML/input/cache-check (alleen CSV vs live)",
    )
    ap.add_argument(
        "--input-dir",
        metavar="DIR",
        default=None,
        help=f"Image-zoekmap (default: {config.INPUT_DIR})",
    )
    ap.add_argument(
        "--no-network",
        action="store_true",
        help="Bij XML-check geen CDN-HEAD / Shopify files-lookup (alleen cache)",
    )
    args = ap.parse_args()

    if args.brand:
        config.apply_brand(args.brand)

    if os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        print(
            "KTM_SKIP_SHOPIFY_API is gezet — dit script heeft live Shopify nodig.",
            file=sys.stderr,
        )
        return 1
    if not config.SHOPIFY_ACCESS_TOKEN:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", file=sys.stderr)
        return 1

    csv_path = args.csv or latest_all_csv(config.PRODUCTS_OUTPUT_DIR)
    if not csv_path or not os.path.isfile(csv_path):
        print(
            "Geen CSV: geef --csv of zet shopify_export_all_*.csv in output/products/.",
            file=sys.stderr,
        )
        return 1

    print(f"CSV: {csv_path}", file=sys.stderr, flush=True)
    split = parse_csv_images_split(csv_path)
    if not split:
        print("Geen Image Src in deze CSV.", file=sys.stderr)
        return 0

    candidates = sorted(h for h, (_real, fb) in split.items() if fb)
    print(
        f"Handles met fallback in export: {len(candidates)}",
        file=sys.stderr,
        flush=True,
    )
    if args.limit and args.limit > 0:
        candidates = candidates[: args.limit]

    print("Live productmedia ophalen…", file=sys.stderr, flush=True)
    live_norms, live_id_by_handle = fetch_handle_maps_for_handles(
        candidates,
        args.workers,
        graphql_batch=args.graphql_batch,
        fetch_workers=args.fetch_workers,
        rest_only=args.rest_only,
    )

    sku_by_handle = _first_sku_from_csv_rows(csv_path)
    xml_by_handle: dict[str, list[str]] = {}
    by_exact: dict[str, list[Path]] | None = None
    by_lower: dict[str, list[Path]] | None = None
    input_root: Path | None = None
    cache: dict | None = None

    if not args.no_xml_check:
        print("XML + lokale images laden…", file=sys.stderr, flush=True)
        products = load_products()
        xml_by_handle = _build_xml_image_index(products)
        input_root = Path(args.input_dir or config.INPUT_DIR).resolve()
        by_exact, by_lower = build_basename_index(input_root)
        cache = load_cache()
        print(
            f"  {len(xml_by_handle)} handles met image-refs in XML; "
            f"input {input_root}",
            file=sys.stderr,
            flush=True,
        )

    writer = csv.writer(sys.stdout, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(
        [
            "Handle",
            "Variant_SKU",
            "Product_id",
            "Live_status",
            "Csv_echte_urls",
            "Xml_resolveerbare_urls",
            "Aanbevolen_actie",
        ]
    )

    n_live_fallback_only = 0
    n_csv_ready = 0
    n_xml_ready = 0
    n_still_missing = 0
    n_not_in_shop = 0
    n_live_has_real = 0

    for handle in candidates:
        real_csv, _fb_csv = split.get(handle, ([], []))
        if handle not in live_id_by_handle:
            n_not_in_shop += 1
            writer.writerow(
                [
                    handle,
                    sku_by_handle.get(handle, ""),
                    "",
                    "niet_in_shop",
                    len(real_csv),
                    "",
                    "controleren_of_product_bestaat",
                ]
            )
            continue

        live = live_norms.get(handle, set())
        pid = live_id_by_handle[handle]
        if not live_norms_are_fallback_only(live):
            if live and any(
                not is_fallback_image_url(n) for n in live
            ):
                n_live_has_real += 1
                live_status = "live_heeft_al_echte_foto"
            else:
                live_status = "geen_live_media"
            writer.writerow(
                [
                    handle,
                    sku_by_handle.get(handle, ""),
                    pid,
                    live_status,
                    len(real_csv),
                    "",
                    "geen_actie_fallback_only",
                ]
            )
            continue

        n_live_fallback_only += 1
        missing_on_live = [u for u in real_csv if norm_src(u) not in live]
        xml_urls: list[str] = []
        if (
            not missing_on_live
            and not args.no_xml_check
            and cache is not None
            and input_root is not None
            and by_exact is not None
            and by_lower is not None
        ):
            refs = xml_by_handle.get(handle, [])
            if refs:
                xml_urls = _resolve_xml_images_for_handle(
                    refs,
                    input_root,
                    by_exact,
                    by_lower,
                    cache,
                    use_network=not args.no_network,
                )

        if missing_on_live:
            n_csv_ready += 1
            actie = "sync_via_shopify_compare_export_images"
        elif xml_urls:
            n_xml_ready += 1
            actie = "herexport_main_of_ensure_image_dan_sync"
        else:
            n_still_missing += 1
            actie = "nog_geen_bron"

        writer.writerow(
            [
                handle,
                sku_by_handle.get(handle, ""),
                pid,
                "live_alleen_fallback",
                len(missing_on_live) if missing_on_live else 0,
                len(xml_urls),
                actie,
            ]
        )

    print(
        f"\nSamenvatting:\n"
        f"  Live alleen fallback: {n_live_fallback_only}\n"
        f"    → CSV heeft echte URL(s) nog niet live: {n_csv_ready}\n"
        f"    → XML+input/cache/CDN resolveerbaar: {n_xml_ready}\n"
        f"    → Nog geen bron gevonden: {n_still_missing}\n"
        f"  Live heeft al echte foto (niet alleen fallback): {n_live_has_real}\n"
        f"  Handle niet in shop: {n_not_in_shop}",
        file=sys.stderr,
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
