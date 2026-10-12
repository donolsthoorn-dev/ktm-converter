"""Bihr-catalogus tegen Motox: discontinued, verdwenen maten, niet meer leverbaar."""

from __future__ import annotations

import re
from pathlib import Path

_PART = re.compile(rb'(?<![A-Za-z])"(?:PartNumber|partNumber)"\s*:\s*"([^"]+)"')
_FLAG = re.compile(rb'"(?:IsDiscontinued|isDiscontinued)"\s*:\s*(true|false)')


def catalog_part_status(catalog_dir: Path) -> tuple[set[str], set[str]]:
    """Alle partnummers en de discontinued subset uit de V3-productcatalogus."""
    files = _product_json_files(catalog_dir)
    catalog: set[str] = set()
    discontinued: set[str] = set()
    for path in files:
        parts, disc = _parts_in_file(path)
        catalog.update(parts)
        discontinued.update(disc)
    return catalog, discontinued


def discontinued_part_numbers(catalog_dir: Path) -> set[str]:
    """Partnummers die in de V3-productcatalogus discontinued zijn."""
    _, discontinued = catalog_part_status(catalog_dir)
    return discontinued


def select_fully_discontinued(
    discontinued: set[str],
    sku_to_product_id: dict,
    products_index: dict,
) -> list[dict]:
    """Actieve producten waarvan elke variant-SKU bij Bihr discontinued is."""
    disc = _upper_set(discontinued)
    chosen: list[dict] = []
    meta_by_pid = _product_meta_by_id(products_index)
    for product_id, skus in _variants_by_product(sku_to_product_id).items():
        meta = meta_by_pid.get(product_id)
        if not meta or meta["status"] != "ACTIVE":
            continue
        if not skus or not all(sku in disc for sku in skus):
            continue
        chosen.append(_choice(product_id, meta, skus))
    chosen.sort(key=lambda item: (item["handle"], item["id"]))
    return chosen


def select_products_without_bihr_offer(
    catalog_parts: set[str],
    discontinued: set[str],
    sku_to_product_id: dict,
    products_index: dict,
) -> list[dict]:
    """Actieve Bihr-producten zonder nog een leverbare variant.

    Een product hoort bij Bihr als minstens één variant-SKU in de catalogus
    staat. Leverbaar betekent: nog in de catalogus en niet discontinued.
    Een maat die uit de catalogus is verdwenen telt niet meer mee. Tijdelijk
    voorraad 0 wel: die variant blijft in de catalogus en het product blijft
    actief, alleen niet bestelbaar.
    """
    catalog = _upper_set(catalog_parts)
    disc = _upper_set(discontinued)
    chosen: list[dict] = []
    meta_by_pid = _product_meta_by_id(products_index)
    for product_id, skus in _variants_by_product(sku_to_product_id).items():
        meta = meta_by_pid.get(product_id)
        if not meta or meta["status"] != "ACTIVE":
            continue
        if not skus or not any(sku in catalog for sku in skus):
            continue
        if any(sku in catalog and sku not in disc for sku in skus):
            continue
        chosen.append(_choice(product_id, meta, skus))
    chosen.sort(key=lambda item: (item["handle"], item["id"]))
    return chosen


def variants_to_block(
    catalog_parts: set[str],
    discontinued: set[str],
    stock_part_numbers: set[str],
    sku_to_product_id: dict,
    sku_to_variant_id: dict,
    sku_to_policy: dict,
    *,
    respect_current: bool,
) -> tuple[dict[str, list[tuple[str, str]]], int]:
    """Varianten die niet meer besteld mogen worden, per product-id.

    Alleen producten die nog minstens één SKU in de Bihr-catalogus hebben.
    Een variant gaat op DENY als hij uit de catalogus is, of discontinued is
    en geen eigen voorraadregel heeft. Voorraadregels blijven bij de
    stock-sync, zodat restvoorraad doorverkocht kan worden.

    Retourneert ``(product_id → [(variant_id, "DENY")], al_deny)``.
    ``al_deny`` telt varianten die al DENY zijn als ``respect_current`` aan staat.
    """
    catalog = _upper_set(catalog_parts)
    disc = _upper_set(discontinued)
    stock = _upper_set(stock_part_numbers)
    blocked: dict[str, list[tuple[str, str]]] = {}
    already_deny = 0
    for product_id, skus in _variants_by_product(sku_to_product_id).items():
        if not any(sku in catalog for sku in skus):
            continue
        seen_vids: dict[str, str] = {}
        for sku in skus:
            if sku in stock:
                continue
            if sku in catalog and sku not in disc:
                continue
            vid = _mapped(sku_to_variant_id, sku)
            if not vid or vid in seen_vids:
                continue
            if respect_current and _policy(sku_to_policy, sku) == "DENY":
                already_deny += 1
                continue
            seen_vids[vid] = "DENY"
        if seen_vids:
            blocked[product_id] = sorted(seen_vids.items())
    return blocked, already_deny


def _product_json_files(catalog_dir: Path) -> list[Path]:
    return [
        path
        for path in sorted(catalog_dir.rglob("*.json"))
        if path.is_file() and "Price" not in path.name and "Stock" not in path.name
    ]


def _upper_set(values: set[str]) -> set[str]:
    return {str(part).strip().upper() for part in values if str(part).strip()}


def _variants_by_product(sku_to_product_id: dict) -> dict[str, set[str]]:
    by_pid: dict[str, set[str]] = {}
    for sku, pid in sku_to_product_id.items():
        part = str(sku).strip().upper()
        product_id = str(pid).strip()
        if part and product_id:
            by_pid.setdefault(product_id, set()).add(part)
    return by_pid


def _product_meta_by_id(products_index: dict) -> dict[str, dict]:
    meta_by_pid: dict[str, dict] = {}
    for handle, meta in products_index.items():
        if not isinstance(meta, dict):
            continue
        product_id = str(meta.get("id") or "").strip()
        if not product_id or product_id in meta_by_pid:
            continue
        meta_by_pid[product_id] = {
            "handle": handle,
            "title": meta.get("title") or "",
            "status": (meta.get("status") or "").strip().upper(),
        }
    return meta_by_pid


def _choice(product_id: str, meta: dict, skus: set[str]) -> dict:
    return {
        "id": product_id,
        "handle": meta["handle"],
        "title": meta["title"],
        "skus": sorted(skus),
    }


def _mapped(mapping: dict, sku: str) -> str:
    for key in (sku, sku.upper()):
        raw = mapping.get(key)
        if raw not in (None, ""):
            return str(raw).strip()
    return ""


def _policy(mapping: dict, sku: str) -> str:
    pol = _mapped(mapping, sku).upper()
    return pol if pol in ("CONTINUE", "DENY") else ""


def _parts_in_file(path: Path) -> tuple[set[str], set[str]]:
    catalog: set[str] = set()
    discontinued: set[str] = set()
    with path.open("rb") as handle:
        buf = b""
        while True:
            chunk = handle.read(4_000_000)
            buf += chunk
            marks = list(_PART.finditer(buf))
            if not chunk:
                end = len(buf)
            elif len(marks) < 2:
                continue
            else:
                end = marks[-1].start()
            usable = [mark for mark in marks if mark.start() < end] if chunk else marks
            for index, mark in enumerate(usable):
                nxt = usable[index + 1].start() if index + 1 < len(usable) else end
                part = mark.group(1).decode().strip().upper()
                if not part:
                    continue
                catalog.add(part)
                flag = _FLAG.search(buf[mark.end() : nxt])
                if flag and flag.group(1) == b"true":
                    discontinued.add(part)
            buf = buf[end:]
            if not chunk:
                break
    return catalog, discontinued
