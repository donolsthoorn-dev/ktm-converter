#!/usr/bin/env python3
"""
Motox e-bihr sync: Bihr V3+VSE → Shopify create + image backfill + prices + fits_on.

fits_on gaat naar nieuwe producten én naar bestaande producten waar de
passendheid leeg is of afwijkt van de VSE-data van deze run.
Goederencode en land van herkomst uit Bihr gaan mee bij het aanmaken, en
worden op bestaande producten bijgewerkt als ze leeg zijn of afwijken.
Oud Bihr-nummer en leverancierscode gaan naar variant-metafields, een
zoeklijst op het product, en ``ref:``-tags zodat de sitezoekfunctie ze vindt.

  # Dry-run tegen bestaande raw:
  python3 scripts/motox_ebihr_sync.py --raw-dir motox/e-bihr/raw/<ts> --dry-run

  # Apply (schrijft naar Motox):
  python3 scripts/motox_ebihr_sync.py --raw-dir motox/e-bihr/raw/<ts> --apply

  # Fetch + apply:
  python3 scripts/motox_ebihr_sync.py --fetch --apply

  # Alleen prijzen (geen creates / image-backfill):
  python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --prices-only

  # Alleen creates (geen price / image-backfill):
  python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --create-only

  # Alleen ontbrekende images backfill + publiceren op alle kanalen:
  python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --images-only --max-image-backfill 50

Prijzen: standaard alleen delta (Bihr ≠ huidige Motox-prijs uit cache).
  --force-all-prices  schrijft alle matched prijzen opnieuw (oude gedrag)
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

from modules.ebihr.article_numbers import plan_article_update  # noqa: E402
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
    _fits_on_hash,
    load_motox_indexes,
    refresh_motox_cache,
)
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("motox_ebihr_sync")


def _normalize_price(value: str | None) -> str:
    """Normaliseer prijs voor vergelijking (2 decimalen). Leeg → ''."""
    raw = (value or "").strip().replace(",", ".")
    if not raw:
        return ""
    try:
        return f"{float(raw):.2f}"
    except ValueError:
        return raw


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


def _resolve_product_id(
    handle: str,
    skus: set[str],
    handle_to_pid: dict[str, str],
    sku_to_pid: dict[str, str],
) -> str:
    pid = (handle_to_pid.get(handle) or "").strip()
    if pid:
        return pid
    for s in skus:
        pid = (sku_to_pid.get(s) or sku_to_pid.get(s.upper()) or "").strip()
        if pid:
            return pid
    return ""


def _article_plan(group: dict, products_index: dict, sku_to_article: dict, handle: str):
    meta = products_index.get(handle) if isinstance(products_index.get(handle), dict) else {}
    stored_by_sku: dict[str, str] = {}
    for variant in group.get("variants") or []:
        sku = (variant.get("sku") or "").strip()
        if not sku:
            continue
        raw = sku_to_article.get(sku.upper()) or sku_to_article.get(sku) or ""
        if raw:
            stored_by_sku[sku.upper()] = raw
    return plan_article_update(
        group.get("variants") or [],
        stored_codes=(meta or {}).get("search_codes") or [],
        stored_by_sku=stored_by_sku,
        stored_ref_tags=(meta or {}).get("ref_tags") or [],
    )


def _cache_has_image(products_index: dict, handle: str, pid: str) -> bool | None:
    """True/False if known from cache; None if field missing (old cache)."""
    meta = products_index.get(handle)
    if isinstance(meta, dict) and "has_image" in meta:
        return bool(meta.get("has_image"))
    for m in products_index.values():
        if isinstance(m, dict) and (m.get("id") or "") == pid and "has_image" in m:
            return bool(m.get("has_image"))
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Motox e-bihr Shopify sync")
    ap.add_argument("--fetch", action="store_true", help="Eerst V3+VSE ophalen")
    ap.add_argument("--raw-dir", type=Path, default=None)
    ap.add_argument("--apply", action="store_true", help="Schrijf naar Shopify")
    ap.add_argument("--dry-run", action="store_true", help="Geen Shopify-writes (default zonder --apply)")
    ap.add_argument("--create-only", action="store_true", help="Alleen nieuwe producten")
    ap.add_argument("--prices-only", action="store_true", help="Alleen prijs-updates")
    ap.add_argument(
        "--images-only",
        action="store_true",
        help="Alleen image-backfill + kanalen-publish voor bestaande producten",
    )
    ap.add_argument("--skip-metafields", action="store_true")
    ap.add_argument("--skip-images", action="store_true")
    ap.add_argument(
        "--skip-publish",
        action="store_true",
        help="Geen publishablePublish / Online Store publish",
    )
    ap.add_argument("--max-create", type=int, default=0, help="Max nieuwe producten (0=all)")
    ap.add_argument("--max-price-updates", type=int, default=0, help="Max producten met price-update (0=all)")
    ap.add_argument(
        "--max-image-backfill",
        type=int,
        default=0,
        help="Max bestaande producten met image-backfill (0=all)",
    )
    ap.add_argument(
        "--force-all-prices",
        action="store_true",
        help="Alle matched prijzen schrijven (geen delta t.o.v. Motox-cache)",
    )
    ap.add_argument("--refresh-cache", action="store_true", default=True)
    ap.add_argument("--no-refresh-cache", action="store_true")
    args = ap.parse_args()

    mode_n = sum(bool(x) for x in (args.create_only, args.prices_only, args.images_only))
    if mode_n > 1:
        raise SystemExit("Kies één van --create-only / --prices-only / --images-only")

    do_create = not args.prices_only and not args.images_only
    do_image_backfill = not args.prices_only and not args.create_only
    do_prices = not args.create_only and not args.images_only
    do_fitment = (
        not args.prices_only
        and not args.images_only
        and not args.create_only
        and not args.skip_metafields
    )
    do_customs = not args.prices_only and not args.images_only and not args.create_only
    do_articles = (
        not args.prices_only
        and not args.images_only
        and not args.skip_metafields
    )
    do_articles_existing = do_articles and not args.create_only

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

    log.info(
        "Raw: %s | dry_run=%s | create=%s image_backfill=%s prices=%s fitment=%s customs=%s articles=%s",
        raw_dir,
        dry_run,
        do_create,
        do_image_backfill,
        do_prices,
        do_fitment,
        do_customs,
        do_articles,
    )

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
    sku_to_shop_price = idx.get("sku_to_price") or {}
    sku_to_customs = idx.get("sku_to_customs") or {}
    sku_to_article = idx.get("sku_to_article") or {}
    products_index = idx.get("products_index") or {}

    admin = MotoxAdmin()
    report_rows: list[dict] = []
    created = 0
    create_errors = 0
    prices_updated = 0
    price_errors = 0
    price_checked = 0
    price_unchanged = 0
    price_products_written = 0
    meta_set = 0
    meta_errors = 0
    fitment_checked = 0
    fitment_unchanged = 0
    fitment_written = 0
    fitment_no_source = 0
    fitment_errors = 0
    customs_checked = 0
    customs_unchanged = 0
    customs_written = 0
    customs_no_source = 0
    customs_errors = 0
    articles_checked = 0
    articles_unchanged = 0
    articles_written = 0
    articles_errors = 0
    articles_def_error = ""

    if do_articles and not dry_run:
        articles_def_error = admin.ensure_article_definitions()
        if articles_def_error:
            log.warning("Artikelnummer-definities: %s", articles_def_error)
    skipped_exists = 0
    skipped_no_image = 0
    images_backfilled = 0
    image_backfill_errors = 0
    publish_ok = 0
    publish_errors = 0
    skipped_already_has_image = 0
    skipped_no_bihr_image = 0

    # --- Creates ---
    if do_create:
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

            if do_articles:
                article_plan = _article_plan(g, {}, {}, handle)
                if article_plan:
                    aerr = admin.apply_article_numbers(
                        pid, article_plan, sku_to_vid, dry_run=dry_run
                    )
                    if aerr:
                        articles_errors += 1
                        report_rows.append(
                            {"action": "articles", "handle": handle, "ok": "0", "detail": aerr}
                        )
                        log.warning("Artikelnummers fail %s: %s", handle, aerr)
                    else:
                        articles_written += 1
                        report_rows.append(
                            {
                                "action": "articles",
                                "handle": handle,
                                "ok": "1",
                                "detail": "created",
                            }
                        )

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

            # Nieuwe producten met images → ACTIVE + alle sales channels
            if g.get("published") and not args.skip_publish and not img_err:
                aerr = admin.ensure_active(pid, dry_run=dry_run)
                perr = admin.publish_to_all_channels(pid, dry_run=dry_run)
                detail = "; ".join(x for x in (aerr, perr) if x)
                if detail:
                    publish_errors += 1
                    report_rows.append(
                        {"action": "publish", "handle": handle, "ok": "0", "detail": detail}
                    )
                else:
                    publish_ok += 1
                    report_rows.append(
                        {"action": "publish", "handle": handle, "ok": "1", "detail": "all_channels"}
                    )

            if not dry_run:
                time.sleep(0.35)

    # --- Image backfill for existing products without media ---
    if do_image_backfill and not args.skip_images:
        if not dry_run and not args.skip_publish:
            admin.list_publication_ids()

        for g in groups:
            handle = normalize_shopify_product_handle(g["handle"]) or g["handle"]
            skus = {(v.get("sku") or "").strip().upper() for v in g["variants"] if v.get("sku")}
            exists = handle in motox_handles or bool(skus & motox_skus)
            if not exists:
                continue
            images = g.get("images") or []
            if not images:
                skipped_no_bihr_image += 1
                continue
            pid = _resolve_product_id(handle, skus, handle_to_pid, sku_to_pid)
            if not pid:
                continue

            has_img = _cache_has_image(products_index, handle, pid)
            if has_img is True:
                skipped_already_has_image += 1
                continue
            if has_img is None:
                live_has, live_err = admin.product_has_media(pid)
                if live_err:
                    image_backfill_errors += 1
                    report_rows.append(
                        {
                            "action": "image_check",
                            "handle": handle,
                            "ok": "0",
                            "detail": live_err,
                        }
                    )
                    continue
                if live_has:
                    skipped_already_has_image += 1
                    continue

            if args.max_image_backfill and images_backfilled >= args.max_image_backfill:
                break

            img_err = admin.attach_images(pid, images, dry_run=dry_run)
            if img_err:
                image_backfill_errors += 1
                report_rows.append(
                    {"action": "image_backfill", "handle": handle, "ok": "0", "detail": img_err}
                )
                log.warning("Image backfill fail %s: %s", handle, img_err)
                continue

            images_backfilled += 1
            report_rows.append(
                {
                    "action": "image_backfill",
                    "handle": handle,
                    "ok": "1",
                    "detail": f"id={pid}; n={len(images[:20])}",
                }
            )
            log.info("Image backfill %s → %s (%s urls)", handle, pid, len(images[:20]))

            if g.get("published") and not args.skip_publish:
                aerr = admin.ensure_active(pid, dry_run=dry_run)
                perr = admin.publish_to_all_channels(pid, dry_run=dry_run)
                detail = "; ".join(x for x in (aerr, perr) if x)
                if detail:
                    publish_errors += 1
                    report_rows.append(
                        {"action": "publish", "handle": handle, "ok": "0", "detail": detail}
                    )
                else:
                    publish_ok += 1
                    report_rows.append(
                        {"action": "publish", "handle": handle, "ok": "1", "detail": "all_channels"}
                    )

            if not dry_run:
                time.sleep(0.35)

    # --- fits_on op bestaande producten: leeg of afwijkend van VSE ---
    if do_fitment:
        hash_by_pid = {
            (meta.get("id") or ""): (meta.get("fits_on_hash") or "")
            for meta in products_index.values()
            if isinstance(meta, dict) and meta.get("id")
        }
        for g in groups:
            handle = normalize_shopify_product_handle(g["handle"]) or g["handle"]
            skus = {(v.get("sku") or "").strip().upper() for v in g["variants"] if v.get("sku")}
            if handle not in motox_handles and not (skus & motox_skus):
                continue
            tokens = [handle] + [v.get("sku") or "" for v in g["variants"]]
            merged = fits_on_from_map(fits_map, tokens)
            if not merged:
                fitment_no_source += 1
                continue
            pid = _resolve_product_id(handle, skus, handle_to_pid, sku_to_pid)
            if not pid:
                continue
            fitment_checked += 1
            desired = _fits_on_hash(merged)
            if hash_by_pid.get(pid) == desired:
                fitment_unchanged += 1
                continue
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
                fitment_errors += 1
                report_rows.append(
                    {"action": "fitment", "handle": handle, "ok": "0", "detail": merr}
                )
                log.warning("Fitment fail %s: %s", handle, merr)
                continue
            fitment_written += 1
            hash_by_pid[pid] = desired
            report_rows.append(
                {"action": "fitment", "handle": handle, "ok": "1", "detail": "updated"}
            )
            if fitment_written % 50 == 0:
                log.info("Fitment bijgewerkt: %s", fitment_written)
            if not dry_run:
                time.sleep(0.2)
        log.info(
            "Fitment: checked=%s unchanged=%s written=%s no_source=%s errors=%s",
            fitment_checked,
            fitment_unchanged,
            fitment_written,
            fitment_no_source,
            fitment_errors,
        )

    # --- Goederencode en land van herkomst op bestaande producten ---
    if do_customs:
        by_product: dict[str, list[tuple[str, str, str]]] = {}
        handle_by_pid: dict[str, str] = {}
        for g in groups:
            handle = normalize_shopify_product_handle(g["handle"]) or g["handle"]
            skus = {(v.get("sku") or "").strip().upper() for v in g["variants"] if v.get("sku")}
            if handle not in motox_handles and not (skus & motox_skus):
                continue
            has_source = False
            for v in g["variants"]:
                sku = (v.get("sku") or "").strip()
                hs = (v.get("hs_code") or "").strip()
                country = (v.get("country") or "").strip()
                if not hs and not country:
                    continue
                has_source = True
                sku_key = sku.upper()
                vid = (sku_to_vid.get(sku) or sku_to_vid.get(sku_key) or "").strip()
                if not vid:
                    continue
                current = sku_to_customs.get(sku_key) or sku_to_customs.get(sku) or {}
                cur_hs = (current.get("hs") or "").strip()
                cur_country = (current.get("country") or "").strip()
                send_hs = hs if hs and hs != cur_hs else ""
                send_country = country if country and country != cur_country else ""
                customs_checked += 1
                if not send_hs and not send_country:
                    customs_unchanged += 1
                    continue
                pid = _resolve_product_id(handle, skus, handle_to_pid, sku_to_pid)
                if not pid:
                    continue
                by_product.setdefault(pid, []).append((vid, send_hs, send_country))
                handle_by_pid[pid] = handle
            if not has_source:
                customs_no_source += 1
        for pid, items in by_product.items():
            err = admin.update_variant_customs(pid, items, dry_run=dry_run)
            handle = handle_by_pid.get(pid) or pid
            if err:
                customs_errors += 1
                report_rows.append(
                    {"action": "customs", "handle": handle, "ok": "0", "detail": err}
                )
                log.warning("Douane fail %s: %s", handle, err)
                continue
            customs_written += len(items)
            report_rows.append(
                {
                    "action": "customs",
                    "handle": handle,
                    "ok": "1",
                    "detail": f"{len(items)} variants",
                }
            )
            if customs_written % 100 == 0:
                log.info("Douane bijgewerkt: %s varianten", customs_written)
            if not dry_run:
                time.sleep(0.15)
        log.info(
            "Douane: checked=%s unchanged=%s written=%s no_source=%s errors=%s",
            customs_checked,
            customs_unchanged,
            customs_written,
            customs_no_source,
            customs_errors,
        )

    # --- Oud nummer + leverancierscode op bestaande producten ---
    if do_articles_existing:
        for g in groups:
            handle = normalize_shopify_product_handle(g["handle"]) or g["handle"]
            skus = {(v.get("sku") or "").strip().upper() for v in g["variants"] if v.get("sku")}
            if handle not in motox_handles and not (skus & motox_skus):
                continue
            articles_checked += 1
            article_plan = _article_plan(g, products_index, sku_to_article, handle)
            if not article_plan:
                articles_unchanged += 1
                continue
            pid = _resolve_product_id(handle, skus, handle_to_pid, sku_to_pid)
            if not pid:
                continue
            aerr = admin.apply_article_numbers(
                pid, article_plan, sku_to_vid, dry_run=dry_run
            )
            if aerr:
                articles_errors += 1
                report_rows.append(
                    {"action": "articles", "handle": handle, "ok": "0", "detail": aerr}
                )
                log.warning("Artikelnummers fail %s: %s", handle, aerr)
                continue
            articles_written += 1
            report_rows.append(
                {"action": "articles", "handle": handle, "ok": "1", "detail": "updated"}
            )
            if articles_written % 50 == 0:
                log.info("Artikelnummers bijgewerkt: %s", articles_written)
            if not dry_run:
                time.sleep(0.15)
        log.info(
            "Artikelnummers: checked=%s unchanged=%s written=%s errors=%s",
            articles_checked,
            articles_unchanged,
            articles_written,
            articles_errors,
        )

    # --- Price updates for existing (delta by default) ---
    if do_prices:
        use_delta = not args.force_all_prices
        if use_delta and not sku_to_shop_price:
            log.warning(
                "Geen sku_to_price in Motox-cache — delta niet mogelijk; "
                "alle matched prijzen worden geschreven. Ververs cache of gebruik --force-all-prices."
            )
            use_delta = False
        elif use_delta:
            log.info(
                "Prijs-delta: Bihr vs Motox-cache (%s SKU-prijzen in cache)",
                len(sku_to_shop_price),
            )
        else:
            log.info("Prijs-modus: force-all (geen delta)")

        sku_price: dict[str, str] = {}
        for g in groups:
            for v in g["variants"]:
                sku = (v.get("sku") or "").strip()
                price = (v.get("price") or "").strip()
                if sku and price:
                    sku_price[sku] = price
                    sku_price[sku.upper()] = price

        by_pid: dict[str, list[tuple[str, str]]] = {}
        for sku, price in list(sku_price.items()):
            if sku != sku.upper() and sku.upper() in sku_price and sku != sku.upper():
                continue
            vid = sku_to_vid.get(sku) or sku_to_vid.get(sku.upper())
            pid = sku_to_pid.get(sku) or sku_to_pid.get(sku.upper())
            if not vid or not pid:
                continue
            bihr_n = _normalize_price(price)
            if not bihr_n:
                continue
            price_checked += 1
            if use_delta:
                shop_raw = (
                    sku_to_shop_price.get(sku)
                    or sku_to_shop_price.get(sku.upper())
                    or ""
                )
                shop_n = _normalize_price(shop_raw)
                if shop_n and shop_n == bihr_n:
                    price_unchanged += 1
                    continue
            by_pid.setdefault(pid, []).append((vid, bihr_n))

        n_price_products = 0
        for pid, pairs in by_pid.items():
            if args.max_price_updates and n_price_products >= args.max_price_updates:
                break
            seen: dict[str, str] = {}
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
                price_products_written += 1
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

        log.info(
            "Prijzen: checked=%s unchanged=%s products_written=%s variants_written=%s errors=%s",
            price_checked,
            price_unchanged,
            price_products_written,
            prices_updated,
            price_errors,
        )

    summary = {
        "dry_run": dry_run,
        "raw_dir": str(raw_dir),
        "groups": len(groups),
        "created": created,
        "create_errors": create_errors,
        "skipped_exists": skipped_exists,
        "skipped_no_image": skipped_no_image,
        "images_backfilled": images_backfilled,
        "image_backfill_errors": image_backfill_errors,
        "skipped_already_has_image": skipped_already_has_image,
        "skipped_no_bihr_image_for_existing": skipped_no_bihr_image,
        "publish_ok": publish_ok,
        "publish_errors": publish_errors,
        "price_checked": price_checked,
        "price_unchanged": price_unchanged,
        "price_products_written": price_products_written,
        "price_variant_updates": prices_updated,
        "price_errors": price_errors,
        "price_delta": not args.force_all_prices,
        "metafields_set": meta_set,
        "metafields_errors": meta_errors,
        "fitment_checked": fitment_checked,
        "fitment_unchanged": fitment_unchanged,
        "fitment_written": fitment_written,
        "fitment_no_source": fitment_no_source,
        "fitment_errors": fitment_errors,
        "customs_checked": customs_checked,
        "customs_unchanged": customs_unchanged,
        "customs_written": customs_written,
        "customs_no_source": customs_no_source,
        "customs_errors": customs_errors,
        "articles_checked": articles_checked,
        "articles_unchanged": articles_unchanged,
        "articles_written": articles_written,
        "articles_errors": articles_errors,
        "articles_def_error": articles_def_error,
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

    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write("## Motox e-bihr sync\n\n")
            f.write(f"- dry_run: `{dry_run}`\n")
            f.write(f"- created: **{created}** (errors {create_errors})")
            if create_errors:
                f.write(" — create errors are warnings (job stays green)\n")
            else:
                f.write("\n")
            f.write(f"- skipped exists: {skipped_exists}, no image: {skipped_no_image}\n")
            f.write(
                f"- image backfill: **{images_backfilled}** "
                f"(errors {image_backfill_errors}, already had {skipped_already_has_image})\n"
            )
            f.write(f"- publish channels: **{publish_ok}** (errors {publish_errors})\n")
            f.write(
                f"- prices: checked **{price_checked}**, unchanged **{price_unchanged}**, "
                f"products written **{price_products_written}**, "
                f"variants written **{prices_updated}** (errors {price_errors})"
                f"{'' if not args.force_all_prices else ' [force-all]'}\n"
            )
            f.write(f"- metafields set: **{meta_set}** (errors {meta_errors})\n")
            f.write(
                f"- fitment existing: checked **{fitment_checked}**, "
                f"unchanged **{fitment_unchanged}**, written **{fitment_written}**, "
                f"no source **{fitment_no_source}** (errors {fitment_errors})\n"
            )
            f.write(
                f"- customs existing: checked **{customs_checked}**, "
                f"unchanged **{customs_unchanged}**, written **{customs_written}**, "
                f"no source **{customs_no_source}** (errors {customs_errors})"
            )
            if customs_errors:
                f.write(" — customs errors are warnings (job stays green)\n")
            else:
                f.write("\n")
            f.write(
                f"- article numbers: checked **{articles_checked}**, "
                f"unchanged **{articles_unchanged}**, written **{articles_written}** "
                f"(errors {articles_errors})\n"
            )
            f.write(f"- report: `{report_path}`\n")

    log.info("Summary: %s", summary)
    print(json.dumps(summary, indent=2))
    print(f"Report: {report_path}")

    # Create / douane / fitment: log + report, but do not fail the nightly job.
    # Price / metafield-write / image / publish errors remain hard failures.
    soft = []
    if create_errors:
        soft.append(f"create={create_errors}")
    if customs_errors:
        soft.append(f"customs={customs_errors}")
    if fitment_errors:
        soft.append(f"fitment={fitment_errors}")
    if articles_errors:
        soft.append(f"articles={articles_errors}")
    if soft:
        log.warning(
            "Non-fatal sync issues (%s); see sync_report. Job stays green.",
            ", ".join(soft),
        )
    hard_errors = (
        price_errors
        or meta_errors
        or image_backfill_errors
        or publish_errors
    )
    if hard_errors:
        return 2 if not dry_run else 0
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
