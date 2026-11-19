"""Bihr-adviesprijs incl. btw voor varianten die al in Motox staan.

Het aanmaakfilter (hardparts zonder offroad-fitment) bepaalt alleen welke
producten nieuw worden. Prijzen gelden voor elke bestaande variant met een
Bihr-partnummer, ook als dat product niet opnieuw aangemaakt zou worden.
KTM, Husqvarna, GasGas en WP blijven op de ktm-shop-prijs.
"""

from __future__ import annotations

import re

from modules.ebihr.build_products import _price_incl

# Zelfde merken als motox_oem_price_sync: die prijs komt van ktm-shop.nl.
OEM_PRICE_VENDORS = frozenset({"ktm", "husqvarna", "gasgas", "wp"})


def vendor_key(vendor: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (vendor or "").lower())


def _positive_retail(price_row: dict | None) -> str:
    raw = _price_incl(price_row, {})
    if not raw:
        return ""
    try:
        if float(raw) <= 0:
            return ""
    except ValueError:
        return ""
    return raw


def _same_price(left: str | None, right: str | None) -> bool:
    def norm(value: str | None) -> str:
        raw = (value or "").strip().replace(",", ".")
        if not raw:
            return ""
        try:
            return f"{float(raw):.2f}"
        except ValueError:
            return raw

    a, b = norm(left), norm(right)
    return bool(a) and a == b


def retail_prices_by_partnumber(price_rows: dict[str, dict]) -> dict[str, str]:
    """Elke Bihr-prijsregel met een adviesprijs incl. btw > 0."""
    out: dict[str, str] = {}
    for pn, row in price_rows.items():
        sku = str(pn or "").strip()
        if not sku or not isinstance(row, dict):
            continue
        price = _positive_retail(row)
        if price:
            out[sku] = price
    return out


def select_price_updates(
    desired: dict[str, str],
    *,
    sku_to_vid: dict[str, str],
    sku_to_pid: dict[str, str],
    sku_to_shop_price: dict[str, str],
    sku_to_vendor: dict[str, str] | None = None,
    use_delta: bool = True,
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, int]]:
    """
    Bestaande Motox-varianten waarvan de Bihr-adviesprijs afwijkt.

    Returns (product_id -> [(variant_id, price)], counters).
    """
    vendors = sku_to_vendor or {}
    by_pid: dict[str, dict[str, str]] = {}
    stats = {
        "checked": 0,
        "unchanged": 0,
        "skipped_oem": 0,
        "queued_variants": 0,
    }
    seen: set[str] = set()
    for sku, price in desired.items():
        key = str(sku or "").strip()
        if not key or not price:
            continue
        upper = key.upper()
        if upper in seen:
            continue
        seen.add(upper)
        vendor = vendors.get(key) or vendors.get(upper) or ""
        if vendor_key(vendor) in OEM_PRICE_VENDORS:
            stats["skipped_oem"] += 1
            continue
        vid = sku_to_vid.get(key) or sku_to_vid.get(upper)
        pid = sku_to_pid.get(key) or sku_to_pid.get(upper)
        if not vid or not pid:
            continue
        stats["checked"] += 1
        if use_delta:
            shop_raw = sku_to_shop_price.get(key) or sku_to_shop_price.get(upper) or ""
            if _same_price(shop_raw, price):
                stats["unchanged"] += 1
                continue
        by_pid.setdefault(pid, {})[vid] = price

    out = {pid: list(pairs.items()) for pid, pairs in by_pid.items()}
    stats["queued_variants"] = sum(len(pairs) for pairs in out.values())
    return out, stats
