"""
XML (cross-brand canonical YMM) → Supabase public.canonical_product_fits_on.

Maps variant SKU's from shopify_variants to merged fitment tuples, then upserts ymm_json + content_hash.
Shopify catalog mirror vult deze tabel niet (alleen products/variants/ETA).
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from modules.cross_brand_ymm import (
    build_canonical_sku_to_ymm,
    build_normalized_sku_ymm_lookup,
    resolve_cross_brand_xml_paths,
    ymm_lookup_for_sku,
)
from modules.metafields_manager_export import _ymm_tuples_to_fits_on_json
from modules.shopify_supabase_mirror import _supabase_upsert
from modules.ymm_content_hash import ymm_json_content_hash
from modules.ymm_export import DEFAULT_YMM_OEM_MAKES, resolve_ymm_make_filter

CANONICAL_FITS_ON_TABLE = "canonical_product_fits_on"

_REQUEST_TIMEOUT = (15, 120)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _payload_bool(payload: dict[str, Any], key: str, default: bool) -> bool:
    raw = payload.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _payload_int(payload: dict[str, Any], key: str, default: int) -> int:
    raw = payload.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _payload_handles(payload: dict[str, Any]) -> set[str]:
    raw = payload.get("only_handles")
    if raw is None:
        return set()
    if isinstance(raw, list):
        vals = [str(x).strip().lower() for x in raw]
    else:
        vals = [str(x).strip().lower() for x in str(raw).split(",") if str(x).strip()]
    return {v for v in vals if v}


def _resolve_product_ids_for_handles(
    supabase_sess: requests.Session,
    rest_base: str,
    headers: dict[str, str],
    handles: set[str],
) -> set[int]:
    if not handles:
        return set()
    quoted = ",".join([f"\"{h}\"" for h in sorted(handles)])
    r = supabase_sess.get(
        f"{rest_base}/shopify_products",
        headers=headers,
        params={
            "select": "shopify_product_id,handle",
            "handle": f"in.({quoted})",
            "limit": "5000",
        },
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    out: set[int] = set()
    for row in r.json():
        try:
            out.add(int(row["shopify_product_id"]))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _fetch_all_variant_skus(
    supabase_sess: requests.Session,
    rest_base: str,
    headers: dict[str, str],
    *,
    product_ids: set[int] | None,
    limit_products: int,
) -> dict[int, list[str]]:
    by_product: dict[int, set[str]] = defaultdict(set)
    offset = 0
    page_size = 1000

    while True:
        params: dict[str, str] = {
            "select": "shopify_product_id,sku",
            "sku": "not.is.null",
            "order": "shopify_product_id.asc",
            "limit": str(page_size),
            "offset": str(offset),
        }
        if product_ids:
            params["shopify_product_id"] = f"in.({','.join(str(x) for x in sorted(product_ids))})"

        r = supabase_sess.get(
            f"{rest_base}/shopify_variants",
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        for row in batch:
            try:
                pid = int(row["shopify_product_id"])
            except (TypeError, ValueError, KeyError):
                continue
            sku = (row.get("sku") or "").strip()
            if sku:
                by_product[pid].add(sku)
        if limit_products > 0 and len(by_product) >= limit_products:
            break
        offset += len(batch)
        if len(batch) < page_size:
            break

    out = {pid: sorted(skus) for pid, skus in by_product.items()}
    if limit_products > 0:
        return dict(list(out.items())[:limit_products])
    return out


def run_sync_canonical_ymm_to_supabase(
    payload: dict[str, Any],
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """
    Build cross-brand YMM from XML and upsert public.canonical_product_fits_on.
    Requires shopify_variants + shopify_products in Supabase (catalog mirror).
    """

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    dry_run = _payload_bool(payload, "dry_run", True)
    limit = max(0, _payload_int(payload, "limit", 0))
    only_handles = _payload_handles(payload)
    ymm_all_makes = _payload_bool(payload, "ymm_all_makes", False)
    filter_makes = resolve_ymm_make_filter(
        payload.get("ymm_makes") if isinstance(payload.get("ymm_makes"), list) else None,
        all_makes=ymm_all_makes,
    )
    if filter_makes is None and not ymm_all_makes:
        filter_makes = set(DEFAULT_YMM_OEM_MAKES)

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "only_handles": len(only_handles),
        "limit": limit,
        "products_with_skus": 0,
        "products_with_ymm": 0,
        "upserted_rows": 0,
        "skipped_no_ymm": 0,
    }

    xml_paths = resolve_cross_brand_xml_paths()
    if not xml_paths:
        return (stats, "Geen CBEXPDN XML-bestanden gevonden (input/ ontbreekt op runner?).")

    _log(
        f"Canonical YMM → Supabase ({'DRY-RUN' if dry_run else 'WRITE'}): "
        f"XML's={[p.split('/')[-1] for p in xml_paths]}, handles={len(only_handles)}, limit={limit}"
    )
    _log("XML inlezen (cross-brand union)…")
    canonical = build_canonical_sku_to_ymm(xml_paths, filter_makes=filter_makes)
    ymm_lookup = build_normalized_sku_ymm_lookup(canonical)
    _log(f"Canonical map: {len(canonical)} SKU's met YMM.")

    product_ids_filter: set[int] | None = None
    if only_handles:
        product_ids_filter = _resolve_product_ids_for_handles(
            supabase_sess, rest_base, supabase_headers, only_handles
        )
        if not product_ids_filter:
            return (stats, f"Geen shopify_products gevonden voor handles: {sorted(only_handles)}")
        _log(f"Handles → {len(product_ids_filter)} shopify_product_id(s).")

    try:
        by_product = _fetch_all_variant_skus(
            supabase_sess,
            rest_base,
            supabase_headers,
            product_ids=product_ids_filter,
            limit_products=limit,
        )
    except requests.RequestException as e:
        return (stats, f"Supabase read shopify_variants failed: {str(e)[:1000]}")

    stats["products_with_skus"] = len(by_product)
    if not by_product:
        return (stats, "Geen varianten in Supabase (eerst shopify_catalog_mirror draaien?).")

    synced = _iso_now()
    upsert_rows: list[dict[str, Any]] = []

    for pid, skus in sorted(by_product.items()):
        ymm_union: set[tuple[str, str, str]] = set()
        for sku in skus:
            ymm_union |= ymm_lookup_for_sku(ymm_lookup, sku)
        if not ymm_union:
            stats["skipped_no_ymm"] += 1
            continue
        stats["products_with_ymm"] += 1
        fits_json_str = _ymm_tuples_to_fits_on_json(ymm_union)
        try:
            ymm_json = json.loads(fits_json_str)
        except json.JSONDecodeError:
            ymm_json = {"_raw": fits_json_str}
        upsert_rows.append(
            {
                "shopify_product_id": pid,
                "ymm_json": ymm_json,
                "content_hash": ymm_json_content_hash(ymm_json),
                "xml_synced_at": synced,
                "updated_at": synced,
            }
        )

    if limit > 0:
        upsert_rows = upsert_rows[:limit]

    stats["upserted_rows"] = len(upsert_rows)

    if dry_run:
        for row in upsert_rows[:20]:
            h_preview = json.dumps(row["ymm_json"], ensure_ascii=False)[:120]
            _log(f"DRY-RUN upsert product_id={row['shopify_product_id']} ymm_json≈{h_preview}…")
        _log(
            f"DRY-RUN klaar: {stats['upserted_rows']} rijen zouden naar {CANONICAL_FITS_ON_TABLE}; "
            f"{stats['skipped_no_ymm']} producten zonder XML-YMM."
        )
        return (stats, None)

    batch_size = 200
    for i in range(0, len(upsert_rows), batch_size):
        chunk = upsert_rows[i : i + batch_size]
        try:
            _supabase_upsert(
                supabase_sess,
                rest_base,
                supabase_headers,
                CANONICAL_FITS_ON_TABLE,
                chunk,
                "shopify_product_id",
            )
        except RuntimeError as e:
            return (stats, str(e)[:2000])
        _log(
            f"Upsert {CANONICAL_FITS_ON_TABLE}: "
            f"{min(i + batch_size, len(upsert_rows))}/{len(upsert_rows)}"
        )

    _log(
        f"Canonical sync klaar: upserted={stats['upserted_rows']}, "
        f"skipped_no_ymm={stats['skipped_no_ymm']}"
    )
    return (stats, None)
