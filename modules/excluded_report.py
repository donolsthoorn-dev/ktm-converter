"""
Rapportage: varianten die niet in shopify_export_all / delta terechtkomen (met redenen).
"""

from __future__ import annotations

import csv
import os
from typing import Iterable

import config
from modules.pricing_loader import normalize_sku_key


def _primary_for_handle(items: list[dict]) -> dict:
    return max(items, key=lambda x: len(x.get("title", "")))


def _price_float(price_raw: str | float | int | None) -> float:
    if price_raw is None or price_raw == "":
        return 0.0
    try:
        return float(price_raw)
    except (TypeError, ValueError):
        return 0.0


def _reasons_not_in_delta_initial(
    p: dict,
    price_index: dict[str, str],
    status_index: dict[str, str],
    *,
    skip_type_reason_if_same_as: str | None = None,
) -> list[str]:
    """Waarom deze variant niet in de eerste delta-lijst belandde (main.py eerste delta-loop)."""
    sku_k = normalize_sku_key(p.get("sku"))
    t = (p.get("type") or "").strip()
    reasons: list[str] = []
    if t in config.DELTA_EXCLUDED_TYPES:
        skip_dup = (
            skip_type_reason_if_same_as is not None and t == skip_type_reason_if_same_as
        )
        if not skip_dup:
            reasons.append(f"type uitgesloten ({t})")
    if _price_float(price_index.get(sku_k, "")) <= 0:
        reasons.append("geen prijs")
    if str(status_index.get(sku_k, "") or "").strip() == "80":
        reasons.append("status 80")
    return reasons


def build_exclusion_reden(
    p: dict,
    *,
    primary_type: str,
    primary_excluded: bool,
    price_index: dict[str, str],
    status_index: dict[str, str],
    sku_in_delta_initial: bool,
    sku_in_delta_after_images: bool,
    sku_in_delta_final: bool,
) -> str:
    """Samengevoegde reden voor kolom 'Reden' (puntkomma-gescheiden)."""
    parts: list[str] = []

    if primary_excluded:
        pt = (primary_type or "").strip() or "(leeg)"
        parts.append(f"type uitgesloten ({pt})")

    if sku_in_delta_final:
        return "; ".join(parts) if parts else ""

    if sku_in_delta_initial and not sku_in_delta_after_images:
        parts.append("geen afbeeldingen (geen bestand onder input/ of geen geldige CDN-URL na upload)")

    if not sku_in_delta_initial:
        r = _reasons_not_in_delta_initial(
            p,
            price_index,
            status_index,
            skip_type_reason_if_same_as=(primary_type or "").strip() if primary_excluded else None,
        )
        if r:
            parts.extend(r)
        else:
            parts.append("geen verkoopbare variant in productgroep (prijs/type/status)")

    if not parts:
        parts.append("niet in delta-finale lijst (na titelherstel of andere filter)")

    # Ontdubbelen, volgorde behouden
    seen: set[str] = set()
    out: list[str] = []
    for x in parts:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return "; ".join(out)


def write_excluded_report(
    filename: str,
    products: Iterable[dict],
    *,
    price_index: dict[str, str],
    status_index: dict[str, str],
    delta_initial_skus: set[str],
    delta_after_images_skus: set[str],
    delta_final_skus: set[str],
) -> None:
    """Schrijf CSV met kolommen Handle, Reden, SKU voor relevante varianten."""
    by_handle: dict[str, list[dict]] = {}
    for p in products:
        h = p.get("handle") or p.get("sku") or ""
        by_handle.setdefault(h, []).append(p)

    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    rows: list[tuple[str, str, str]] = []

    for handle, items in by_handle.items():
        primary = _primary_for_handle(items)
        primary_type = (primary.get("type") or "").strip()
        primary_excluded = primary_type in config.DELTA_EXCLUDED_TYPES

        for p in items:
            sku = (p.get("sku") or "").strip()
            if not sku:
                continue
            sku_k = normalize_sku_key(sku)

            in_i = sku_k in delta_initial_skus
            in_img = sku_k in delta_after_images_skus
            in_final = sku_k in delta_final_skus

            # Rij nodig als: niet in delta-exportlijst, of wel in lijst maar door export-filter niet in CSV
            if in_final and not primary_excluded:
                continue

            reden = build_exclusion_reden(
                p,
                primary_type=primary_type,
                primary_excluded=primary_excluded,
                price_index=price_index,
                status_index=status_index,
                sku_in_delta_initial=in_i,
                sku_in_delta_after_images=in_img,
                sku_in_delta_final=in_final,
            )
            if not reden.strip():
                continue
            rows.append((handle, reden, sku))

    rows.sort(key=lambda t: (t[0].lower(), t[2]))

    with open(filename, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Handle", "Reden", "SKU"])
        w.writerows(rows)
