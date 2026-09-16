#!/usr/bin/env python3
"""
Motox e-bihr sync: Bihr V3+VSE → Shopify create + price updates + fits_on metafields.

  # Dry-run tegen bestaande raw:
  python3 scripts/motox_ebihr_sync.py --raw-dir motox/e-bihr/raw/<ts> --dry-run

  # Apply (schrijft naar Motox):
  python3 scripts/motox_ebihr_sync.py --raw-dir motox/e-bihr/raw/<ts> --apply

  # Fetch + apply:
  python3 scripts/motox_ebihr_sync.py --fetch --apply

  # Alleen prijzen (geen creates):
  python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --prices-only

  # Alleen creates (geen price updates):
  python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --create-only
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.build_products import build_product_groups  # noqa: E402
from modules.ebihr.build_ymm import build_ymm_from_raw  # noqa: E402
from modules.ebihr.client import BihrClient  # noqa: E402
from modules.ebihr.constants import OUTPUT_ROOT, RAW_ROOT  # noqa: E402
from modules.ebihr.motox_publish import (  # noqa: E402
    MotoxAdmin,
    fits_on_from_map,
)
from modules.metafields_manager_export import (  # noqa: E402
    _pipe_join_sorted,
    _ymm_summary,
)
from modules.motox_shopify_cache import (  # noqa: E402
    load_motox_indexes,
    refresh_motox_cache,
)
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("motox_ebihr_sync")


def _latest_raw() -> Path | None:
    if not RAW_ROOT.is_dir():
        return None
    dirs = [p for p in RAW_ROOT.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)


def _json_tuples(fits: dict) -> set[tuple[str, str, str]]:
    out: set[tuple[str, str, str]] = set()
    for make, models in fits.items():
        if not isinstance(models, dict):
            continue
        for model, years in models.items():
            for y in years if isinstance(years, list) else []:
                out.add((str(make), str(model), str(y)))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Motox e-bihr Shopify sync")
    ap.add_argument("--fetch", action="store_true", help="Eerst V3+VSE ophalen")
    ap.add_argument("--raw-dir", type=Path, default=None)
    ap.add_argument("--apply", action="store_true", help="Schrijf naar Shopify")
    ap.add_argument("--dry-run", action="store_true", help="Geen Shopify-writes (default zonder --apply)")
    ap.add_argument("--create-only", action="store_true", help="Alleen nieuwe producten")
    ap.add_argument("--prices-only", action="store_true", help="Alleen prijs-updates")
    ap.add_argument("--skip-metafields", action="store_true")
    ap.add_argument("--skip-images", action="store_true")
    ap.add_argument("--max-create", type=int, default=0, help="Max nieuwe producten (0=all)")
    ap.add_argument("--max-price-updates", type=int, default=0, help="Max producten met price-update (0=all)")
    ap.add_argument("--refresh-cache", action="store_true", default=True)
    ap.add_argument("--no-refresh-cache", action="store_true")
    args = ap.parse_args()

    dry_run = not args.apply or args.dry_run
    if args.apply and args.dry_run:
        dry_run = True

    raw_dir = args.raw_dir
    if args.fetch:
        raw_dir = raw_dir or (RAW_ROOT / str(int(time.time())))
        BihrClient().fetch_all(raw_dir)
    if raw_dir is None:
        raw_dir = _latest_raw()
    if raw_dir is None or not raw_dir.is_dir():
        raise SystemExit("Geen raw-dir. Gebruik --fetch of --raw-dir.")

    log.info("Raw: %s | dry_run=%s", raw_dir, dry_run)

    ymm = build_ymm_from_raw(raw_dir)
    fits_map = ymm["fits_on"]
    offroad = ymm["offroad_partnumbers"]
    groups, prices, _stocks, build_stats = build_product_groups(
        raw_dir, offroad_partnumbers=offroad
    )
    log.info("Build stats: %s", build_stats)

    if not args.no_refresh_cache:
        refresh_motox_cache(force=True)
    idx = load_motox_indexes()
    motox_handles = idx["handles"]
    motox_skus = idx["skus"]
    handle_to_pid = idx["handle_to_product_id"]
    sku_to_pid = idx["sku_to_product_id"]
    sku_to_vid = idx.get("sku_to_variant_id") or {}

    admin = MotoxAdmin()
    report_rows: list[dict] = []
    created = 0
    create_errors = 0
    prices_updated = 0
    price_errors = 0
    prices_unchanged = 0
    meta_set = 0
    meta_errors = 0
    skipped_exists = 0
    skipped_no_image = 0

    # --- Creates ---
    if not args.prices_only:
        for g in groups:
            handle = normalize_shopify_product_handle(g["handle"]) or g["handle"]
            skus = {(v.get("sku") or "").strip().upper() for v in g["variants"] if v.get("sku")}
            exists = handle in motox_handles or bool(skus & motox_skus)
            if exists:
                skipped_exists += 1
                continue
            if not (g.get("images") or []):
                skipped_no_image += 1
                continue
            if args.max_create and created >= args.max_create:
                break

            pid, err = admin.product_set_create(g, dry_run=dry_run)
            if err or not pid:
                create_errors += 1
                report_rows.append(
                    {"action": "create", "handle": handle, "ok": "0", "detail": err or "fail"}
                )
                log.warning("Create fail %s: %s", handle, err)
                continue

            img_err = ""
            if not args.skip_images:
                img_err = admin.attach_images(pid, g.get("images") or [], dry_run=dry_run)
            created += 1
            report_rows.append(
                {
                    "action": "create",
                    "handle": handle,
                    "ok": "1",
                    "detail": f"id={pid}" + (f"; images:{img_err}" if img_err else ""),
                }
            )
            log.info("Created %s → %s", handle, pid)

            # Metafields for new product
            if not args.skip_metafields:
                tokens = [handle] + [v.get("sku") or "" for v in g["variants"]]
                merged = fits_on_from_map(fits_map, tokens)
                if merged:
                    fo_json = json.dumps(merged, ensure_ascii=False)
                    tuples = _json_tuples(merged)
                    years = {y for _, _, y in tuples}
                    makes = {m for m, _, _ in tuples}
                    models = {m for _, m, _ in tuples}
                    merr = admin.set_ymm_metafields(
                        pid,
                        fits_on_json=fo_json,
                        ymm_summary=_ymm_summary(tuples) if tuples else "",
                        fits_on_year=_pipe_join_sorted(years),
                        fits_on_make=_pipe_join_sorted(makes),
                        fits_on_model=_pipe_join_sorted(models),
                        dry_run=dry_run,
                    )
                    if merr:
                        meta_errors += 1
                        report_rows.append(
                            {"action": "metafields", "handle": handle, "ok": "0", "detail": merr}
                        )
                    else:
                        meta_set += 1

            # throttle Shopify
            if not dry_run:
                time.sleep(0.35)

    # --- Price updates for existing ---
    if not args.create_only:
        # Build sku -> price from groups
        sku_price: dict[str, str] = {}
        sku_product_handle: dict[str, str] = {}
        for g in groups:
            for v in g["variants"]:
                sku = (v.get("sku") or "").strip()
                price = (v.get("price") or "").strip()
                if sku and price:
                    sku_price[sku] = price
                    sku_price[sku.upper()] = price
                    sku_product_handle[sku] = g["handle"]

        # Group updates by product id
        by_pid: dict[str, list[tuple[str, str]]] = {}
        for sku, price in list(sku_price.items()):
            if sku != sku.upper() and sku.upper() in sku_price and sku != sku.upper():
                continue  # skip duplicate lower keys processed via upper
            vid = sku_to_vid.get(sku) or sku_to_vid.get(sku.upper())
            pid = sku_to_pid.get(sku) or sku_to_pid.get(sku.upper())
            if not vid or not pid:
                continue
            by_pid.setdefault(pid, []).append((vid, price))

        n_price_products = 0
        for pid, pairs in by_pid.items():
            if args.max_price_updates and n_price_products >= args.max_price_updates:
                break
            # dedupe variant ids
            seen = {}
            for vid, price in pairs:
                seen[vid] = price
            pairs = list(seen.items())
            err = admin.update_variant_prices(pid, pairs, dry_run=dry_run)
            n_price_products += 1
            if err:
                price_errors += 1
                report_rows.append(
                    {"action": "price", "handle": pid, "ok": "0", "detail": err}
                )
            else:
                prices_updated += len(pairs)
                report_rows.append(
                    {
                        "action": "price",
                        "handle": pid,
                        "ok": "1",
                        "detail": f"{len(pairs)} variants",
                    }
                )
            if not dry_run:
                time.sleep(0.25)

        prices_unchanged = max(0, len(by_pid) - n_price_products)

    # --- Metafields for existing products that have fitment (optional fill) ---
    # Only for creates above unless --prices-only; skip bulk metafield backfill in v1 nightly
    # to keep runtime bounded. Creates already set metafields.

    summary = {
        "dry_run": dry_run,
        "raw_dir": str(raw_dir),
        "groups": len(groups),
        "created": created,
        "create_errors": create_errors,
        "skipped_exists": skipped_exists,
        "skipped_no_image": skipped_no_image,
        "price_variant_updates": prices_updated,
        "price_errors": price_errors,
        "metafields_set": meta_set,
        "metafields_errors": meta_errors,
        "build": build_stats,
    }
    out_dir = OUTPUT_ROOT / "sync"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    summary_path = out_dir / f"sync_summary_{ts}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    report_path = out_dir / f"sync_report_{ts}.csv"
    with report_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["action", "handle", "ok", "detail"], lineterminator="\n")
        w.writeheader()
        w.writerows(report_rows)

    # GitHub Step Summary
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write("## Motox e-bihr sync\n\n")
            f.write(f"- dry_run: `{dry_run}`\n")
            f.write(f"- created: **{created}** (errors {create_errors})\n")
            f.write(f"- skipped exists: {skipped_exists}, no image: {skipped_no_image}\n")
            f.write(f"- price variant updates: **{prices_updated}** (errors {price_errors})\n")
            f.write(f"- metafields set: **{meta_set}** (errors {meta_errors})\n")
            f.write(f"- report: `{report_path}`\n")

    log.info("Summary: %s", summary)
    print(json.dumps(summary, indent=2))
    print(f"Report: {report_path}")

    if create_errors or price_errors or meta_errors:
        return 2 if not dry_run else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
