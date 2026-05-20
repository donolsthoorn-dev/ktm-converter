"""
Cross-brand YMM: union fitment per SKU from KTM + HSQ + WP CBEXPDN XML files.

Shared spare parts (same variant SKU on a54029994500 / hsq-… / wp-…) get the same
fits_on when exporting metafields or YMM rows per Shopify handle.
"""

from __future__ import annotations

import os
from collections import defaultdict

from modules.brand_config import VALID_BRAND_IDS
from modules.ymm_export import (
    build_merged_sku_to_ymm,
    merge_sku_ymm_maps,
    stream_xml_for_export,
)


def normalize_fitment_sku(sku: str) -> str:
    """Lookup key for variant SKU across brands (case-insensitive)."""
    return (sku or "").strip().upper()


def resolve_cross_brand_xml_paths(
    brand_ids: tuple[str, ...] = ("ktm", "hsq", "wp"),
) -> list[str]:
    """Existing CBEXPDN XML paths per brand (env override + newest glob)."""
    from config import resolve_brand_xml_file

    paths: list[str] = []
    for bid in brand_ids:
        if bid not in VALID_BRAND_IDS:
            continue
        path = resolve_brand_xml_file(bid)
        if os.path.isfile(path):
            paths.append(path)
    return paths


def build_canonical_sku_to_ymm(
    xml_paths: list[str] | None = None,
    *,
    filter_makes: set[str] | None = None,
) -> dict[str, set[tuple[str, str, str]]]:
    """
    Union of build_merged_sku_to_ymm() over all given XML files (default: KTM+HSQ+WP).
    Keys are SKU strings as in XML (PRODUKT_NAME); use ymm_lookup_for_sku() for lookup.
    """
    paths = xml_paths if xml_paths is not None else resolve_cross_brand_xml_paths()
    maps: list[dict[str, set[tuple[str, str, str]]]] = []
    for path in paths:
        structure_index, relations = stream_xml_for_export(xml_path=path)
        maps.append(
            build_merged_sku_to_ymm(structure_index, relations, xml_file=path)
        )
    merged = merge_sku_ymm_maps(*maps) if maps else {}
    if not filter_makes:
        return merged
    filtered: dict[str, set[tuple[str, str, str]]] = {}
    for sku, tset in merged.items():
        kept = {t for t in tset if t[0] in filter_makes}
        if kept:
            filtered[sku] = kept
    return filtered


def build_normalized_sku_ymm_lookup(
    sku_to_ymm: dict[str, set[tuple[str, str, str]]],
) -> dict[str, set[tuple[str, str, str]]]:
    """Uppercase SKU → union of all XML key variants for that SKU."""
    out: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for sku, tset in sku_to_ymm.items():
        key = normalize_fitment_sku(sku)
        if key:
            out[key] |= tset
    return dict(out)


def ymm_lookup_for_sku(
    lookup: dict[str, set[tuple[str, str, str]]],
    sku: str,
) -> set[tuple[str, str, str]]:
    return lookup.get(normalize_fitment_sku(sku), set())


def makes_in_ymm_set(ymm_set: set[tuple[str, str, str]]) -> set[str]:
    return {t[0] for t in ymm_set}
