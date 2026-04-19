"""
Zelfde delta-pipeline als main.py (zonder CSV te schrijven), voor SKU-diagnose.
"""

from __future__ import annotations

import copy
from pathlib import Path

import config
from modules.excluded_report import _primary_for_handle, build_exclusion_reden
from modules.image_manager import load_cache, resolve_image_url_without_upload
from modules.image_resolve import build_basename_index, resolve_local_image
from modules.pricing_loader import load_price_index, normalize_sku_key
from modules.xml_loader import load_products


def _norm_ref_key(s: str) -> str:
    return s.strip().replace("\\", "/").lower()


def _attach_pricing(products: list[dict], price_index, barcode_index, status_index) -> None:
    for p in products:
        sku_key = normalize_sku_key(p["sku"])
        p["price"] = price_index.get(sku_key, "")
        p["barcode"] = barcode_index.get(sku_key, "")
        p["article_status"] = status_index.get(sku_key, "")
        p["product_category"] = p.get("category", "")
        p["type"] = p.get("type", "")


def compute_etl_pipeline_sets(
    products: list[dict],
    price_index: dict[str, str],
    status_index: dict[str, str],
    *,
    use_network: bool = True,
) -> tuple[set[str], set[str], set[str]]:
    """
    Repliceert main.py: delta → images (zonder upload) → titelherstel.
    Retourneert (delta_initial_skus, delta_after_images_skus, delta_final_skus).
    """
    excluded_types = config.DELTA_EXCLUDED_TYPES
    products_by_handle: dict[str, list[dict]] = {}
    for p in products:
        products_by_handle.setdefault(p["handle"], []).append(p)

    delta_products: list[dict] = []

    for handle, items in products_by_handle.items():
        delta_variant_exists = False
        for p in items:
            sku_key = normalize_sku_key(p["sku"])
            if (
                float(price_index.get(sku_key, 0) or 0) > 0
                and p.get("type") not in excluded_types
                and status_index.get(sku_key) != "80"
            ):
                delta_variant_exists = True
                break

        if delta_variant_exists:
            for p in items:
                sku_key = normalize_sku_key(p["sku"])
                if float(price_index.get(sku_key, 0) or 0) > 0 and status_index.get(sku_key) != "80":
                    delta_products.append(p)

    delta_initial_skus = {normalize_sku_key(p["sku"]) for p in delta_products if p.get("sku")}

    cache = load_cache()
    input_root = Path("input")
    by_basename_exact, by_basename_lower = build_basename_index(input_root)

    image_refs: set[str] = set()
    for p in products:
        for img in p.get("images", []):
            s = (img or "").strip()
            if s:
                image_refs.add(s)

    image_url_map: dict[str, str] = {}
    image_url_by_norm: dict[str, str] = {}

    for ref in image_refs:
        local_path = resolve_local_image(ref, input_root, by_basename_exact, by_basename_lower)
        if not local_path:
            continue
        cache_name = local_path.name
        url = resolve_image_url_without_upload(
            cache_name, local_path, cache, use_network=use_network
        )
        if url:
            image_url_map[ref] = url
            image_url_by_norm[_norm_ref_key(ref)] = url

    products_by_handle = {}
    for p in delta_products:
        products_by_handle.setdefault(p["handle"], []).append(p)

    filtered_delta: list[dict] = []

    for handle, items in products_by_handle.items():
        group_has_images = False
        for p in items:
            new_images = []
            for img in p.get("images", []):
                raw = (img or "").strip()
                url = image_url_map.get(raw) if raw else None
                if not url and raw:
                    url = image_url_by_norm.get(_norm_ref_key(raw))
                if url:
                    new_images.append(url)
            p["images"] = new_images
            if new_images:
                group_has_images = True
        if group_has_images:
            filtered_delta.extend(items)

    delta_after_images_skus = {normalize_sku_key(p["sku"]) for p in filtered_delta if p.get("sku")}
    delta_products = filtered_delta

    products_by_handle = {}
    for p in delta_products:
        products_by_handle.setdefault(p["handle"], []).append(p)

    fixed_delta: list[dict] = []
    for handle, items in products_by_handle.items():
        has_title = any(p.get("title") for p in items)
        if not has_title:
            for p in products:
                if p["handle"] == handle:
                    fixed_delta.append(p)
        else:
            fixed_delta.extend(items)

    delta_final_skus = {normalize_sku_key(p["sku"]) for p in fixed_delta if p.get("sku")}
    return delta_initial_skus, delta_after_images_skus, delta_final_skus


def find_variant_by_sku(products: list[dict], sku_query: str) -> dict | None:
    q = sku_query.strip().upper()
    for p in products:
        if str(p.get("sku") or "").strip().upper() == q:
            return p
    return None


def load_catalog_with_pricing() -> tuple[list[dict], dict[str, str], dict[str, str]]:
    """XML + 0150-index, zelfde als main vóór ETL-export."""
    products = load_products()
    price_index, barcode_index, status_index = load_price_index()
    _attach_pricing(products, price_index, barcode_index, status_index)
    return products, price_index, status_index


def analyze_sku(sku_query: str, *, use_network: bool = True) -> dict:
    """
    Voer pipeline uit op deepcopy en geef status + reden voor één SKU.
    """
    products, price_index, status_index = load_catalog_with_pricing()
    p = find_variant_by_sku(products, sku_query)
    if not p:
        return {"found": False, "sku_query": sku_query.strip()}

    wk = copy.deepcopy(products)
    di, da, df = compute_etl_pipeline_sets(wk, price_index, status_index, use_network=use_network)

    handle = p.get("handle") or ""
    sku = str(p.get("sku") or "").strip()
    sku_k = normalize_sku_key(sku)
    by_handle: dict[str, list[dict]] = {}
    for x in products:
        by_handle.setdefault(x.get("handle") or "", []).append(x)
    items = by_handle.get(handle, [])
    primary = _primary_for_handle(items) if items else p
    primary_type = (primary.get("type") or "").strip()
    primary_excluded = primary_type in config.DELTA_EXCLUDED_TYPES

    in_all_csv = not primary_excluded
    in_delta_csv = sku_k in df and not primary_excluded

    reden = ""
    if not in_delta_csv:
        reden = build_exclusion_reden(
            p,
            primary_type=primary_type,
            primary_excluded=primary_excluded,
            price_index=price_index,
            status_index=status_index,
            sku_in_delta_initial=sku_k in di,
            sku_in_delta_after_images=sku_k in da,
            sku_in_delta_final=sku_k in df,
        )

    return {
        "found": True,
        "sku": sku,
        "handle": handle,
        "title": (p.get("title") or "").strip(),
        "type": (p.get("type") or "").strip(),
        "primary_type": primary_type,
        "in_all_csv": in_all_csv,
        "in_delta_csv": in_delta_csv,
        "reden": reden,
    }
