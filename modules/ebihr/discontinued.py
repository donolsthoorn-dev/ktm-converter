"""Bihr-artikelen met IsDiscontinued, gekoppeld aan live Motox-producten."""

from __future__ import annotations

import re
from pathlib import Path

_PART = re.compile(rb'(?<![A-Za-z])"PartNumber"\s*:\s*"([^"]+)"')
_FLAG = re.compile(rb'"IsDiscontinued"\s*:\s*(true|false)')


def discontinued_part_numbers(catalog_dir: Path) -> set[str]:
    """Partnummers die in de V3-productcatalogus discontinued zijn."""
    files = [
        path
        for path in sorted(catalog_dir.rglob("*.json"))
        if path.is_file() and "Price" not in path.name and "Stock" not in path.name
    ]
    found: set[str] = set()
    for path in files:
        found.update(_discontinued_in_file(path))
    return found


def select_fully_discontinued(
    discontinued: set[str],
    sku_to_product_id: dict,
    products_index: dict,
) -> list[dict]:
    """Actieve producten waarvan elke variant-SKU bij Bihr discontinued is."""
    disc = {str(part).strip().upper() for part in discontinued if str(part).strip()}
    by_pid: dict[str, set[str]] = {}
    for sku, pid in sku_to_product_id.items():
        part = str(sku).strip().upper()
        product_id = str(pid).strip()
        if part and product_id:
            by_pid.setdefault(product_id, set()).add(part)

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

    chosen: list[dict] = []
    for product_id, skus in by_pid.items():
        meta = meta_by_pid.get(product_id)
        if not meta or meta["status"] != "ACTIVE":
            continue
        if not skus or not all(sku in disc for sku in skus):
            continue
        chosen.append(
            {
                "id": product_id,
                "handle": meta["handle"],
                "title": meta["title"],
                "skus": sorted(skus),
            }
        )
    chosen.sort(key=lambda item: (item["handle"], item["id"]))
    return chosen


def _discontinued_in_file(path: Path) -> set[str]:
    found: set[str] = set()
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
                flag = _FLAG.search(buf[mark.end() : nxt])
                if flag and flag.group(1) == b"true":
                    found.add(mark.group(1).decode().strip().upper())
            buf = buf[end:]
            if not chunk:
                break
    return found
