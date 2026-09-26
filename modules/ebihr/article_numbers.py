"""Bihr-artikelnummers naast de SKU: oud nummer, leverancierscode, zoektags.

De Shopify-SKU blijft het huidige PartNumber. Extra nummers gaan naar een
variant-metafield (weergave) en naar producttags ``ref:<nummer>`` (sitezoeken).
Een nummer dat Bihr later weglaat of vervangt blijft in de zoeklijst staan.
"""

from __future__ import annotations

import json

REF_PREFIX = "ref:"
_MIN_SEARCH_LEN = 3
_CODE_KEYS = ("old", "supplier")


def clean_code(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.casefold() in {"null", "none", "0"}:
        return ""
    return text


def article_payload(part: object, old: object, supplier: object) -> dict[str, str]:
    """Huidige labels. Leeg of gelijk aan de SKU telt niet als extra nummer."""
    part_key = clean_code(part).casefold()
    out: dict[str, str] = {}
    old_code = clean_code(old)
    if old_code and old_code.casefold() != part_key:
        out["old"] = old_code
    supplier_code = clean_code(supplier)
    if (
        supplier_code
        and supplier_code.casefold() != part_key
        and supplier_code.casefold() != old_code.casefold()
    ):
        out["supplier"] = supplier_code
    return out


def parse_article(raw: object) -> dict[str, str]:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            return {}
    if not isinstance(raw, dict):
        return {}
    return article_payload(raw.get("part") or "", raw.get("old"), raw.get("supplier"))


def _as_article(payload: object) -> dict[str, str]:
    if isinstance(payload, dict):
        return article_payload("", payload.get("old"), payload.get("supplier"))
    return parse_article(payload)


def canonical_article(payload: object) -> str:
    parsed = _as_article(payload)
    if not parsed:
        return ""
    return json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def merge_article(stored: object, fresh: object) -> dict[str, str]:
    """Verse Bihr-labels winnen. Een leeg veld laat het laatst bekende nummer staan."""
    previous = parse_article(stored)
    current = _as_article(fresh)
    out: dict[str, str] = {}
    for key in _CODE_KEYS:
        value = current.get(key) or previous.get(key) or ""
        if value:
            out[key] = value
    return out


def searchable_code(code: object, sku_keys: set[str]) -> str:
    """Nummer dat niet al via de SKU gevonden wordt en als tag mag."""
    text = clean_code(code)
    if len(text) < _MIN_SEARCH_LEN or "," in text or len(text) > 250:
        return ""
    if text.casefold() in sku_keys:
        return ""
    return text


def merge_search_codes(*groups: object, sku_keys: set[str] | None = None) -> list[str]:
    """Unieke zoeknummers, stabiel gesorteerd. Bestaande nummers blijven."""
    blocked = sku_keys or set()
    found: dict[str, str] = {}
    for group in groups:
        if isinstance(group, dict):
            values = [group.get(key) for key in _CODE_KEYS]
        elif isinstance(group, (list, tuple, set)):
            values = list(group)
        else:
            values = [group]
        for value in values:
            code = searchable_code(value, blocked)
            if code:
                found.setdefault(code.casefold(), code)
    return [found[key] for key in sorted(found)]


def _cap_search_codes(stored: list[str], merged: list[str], limit: int = 128) -> list[str]:
    """Shopify-lijstmetafields houden max 128 waarden. Eerder opgeslagen nummers eerst."""
    if len(merged) <= limit:
        return merged
    merged_keys = {code.casefold() for code in merged}
    kept: list[str] = []
    seen: set[str] = set()
    for code in list(stored) + list(merged):
        if len(kept) >= limit:
            break
        key = code.casefold()
        if key not in merged_keys or key in seen:
            continue
        seen.add(key)
        kept.append(code)
    return kept


def ref_tag(code: str) -> str:
    return f"{REF_PREFIX}{code}"


def is_ref_tag(tag: object) -> bool:
    return str(tag or "").casefold().startswith(REF_PREFIX)


def parse_code_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    return [clean_code(item) for item in raw if clean_code(item)]


def plan_article_update(
    variants: list[dict],
    *,
    stored_codes: object = None,
    stored_by_sku: dict | None = None,
    stored_ref_tags: object = None,
) -> dict | None:
    """
    Wat er naar Shopify moet.

    ``None`` als variant-labels, zoeklijst en ``ref:``-tags al kloppen.
    ``variants`` is sku → JSON-string voor metafields die afwijken.
    ``search_codes`` is de nieuwe lijst, of ``None`` als die al gelijk is.
    ``tags`` zijn alleen de nog ontbrekende ``ref:``-tags.
    """
    stored_by_sku = stored_by_sku or {}
    sku_keys = {
        clean_code(variant.get("sku")).casefold()
        for variant in variants
        if clean_code(variant.get("sku"))
    }
    stored_lookup = {
        str(sku).casefold(): raw for sku, raw in stored_by_sku.items() if str(sku).strip()
    }

    variant_writes: dict[str, str] = {}
    harvested: list[str] = list(parse_code_list(stored_codes))
    for variant in variants:
        sku = clean_code(variant.get("sku"))
        if not sku:
            continue
        stored_raw = stored_lookup.get(sku.casefold(), "")
        fresh = variant.get("article_numbers") or {}
        merged = merge_article(stored_raw, fresh)
        harvested.extend(parse_article(stored_raw).values())
        harvested.extend(merged.values())
        desired = canonical_article(merged)
        if desired != canonical_article(stored_raw):
            if desired:
                variant_writes[sku] = desired

    merged_codes = _cap_search_codes(
        parse_code_list(stored_codes),
        merge_search_codes(harvested, sku_keys=sku_keys),
    )
    codes_changed = {code.casefold() for code in merged_codes} != {
        code.casefold() for code in parse_code_list(stored_codes)
    }
    have_tags = {
        str(tag).casefold()
        for tag in (stored_ref_tags or [])
        if is_ref_tag(tag)
    }
    tags = [ref_tag(code) for code in merged_codes if ref_tag(code).casefold() not in have_tags]
    if not variant_writes and not codes_changed and not tags:
        return None
    return {
        "variants": variant_writes,
        "search_codes": merged_codes if codes_changed else None,
        "tags": tags,
    }
