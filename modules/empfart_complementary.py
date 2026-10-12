"""
CBEXPDN product relations → Shopify complementary / ET metafield targets.

XML BEZIEHUNGSTYP:
  MUSSARTIKEL  — required companions (first in complementary)
  EMPFART2ART  — recommended companions (complementary)
  ET2ARTIKEL   — spare/component parts (custom et_articles metafield)

Shopify complementary uses Search & Discovery:
  namespace shopify--discovery--product_recommendation
  key complementary_products (list.product_reference)
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from lxml import etree

from modules.cross_brand_ymm import resolve_cross_brand_xml_paths

RELATION_MUSS = "MUSSARTIKEL"
RELATION_EMPF = "EMPFART2ART"
RELATION_ET = "ET2ARTIKEL"
RELATION_TYPES = (RELATION_MUSS, RELATION_EMPF, RELATION_ET)

COMPLEMENTARY_NAMESPACE = "shopify--discovery--product_recommendation"
COMPLEMENTARY_KEY = "complementary_products"
COMPLEMENTARY_TYPE = "list.product_reference"

ET_NAMESPACE = "global"
ET_KEY = "et_articles"
ET_TYPE = "list.product_reference"
ET_DEFINITION_NAME = "Bijbehorende onderdelen"

DEFAULT_MAX_PRODUCTS = 10


@dataclass
class OwnerRelations:
    """Per owner SKU: ordered related SKUs by relation type."""

    muss: list[str] = field(default_factory=list)
    empf: list[str] = field(default_factory=list)
    et: list[str] = field(default_factory=list)

    def complementary_skus(self) -> list[str]:
        """MUSS first, then EMPF; unique, order preserved."""
        return _unique_preserve(self.muss + self.empf)

    def has_complementary(self) -> bool:
        return bool(self.muss or self.empf)

    def has_et(self) -> bool:
        return bool(self.et)


def _unique_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extend_unique(dest: list[str], extra: list[str]) -> None:
    seen = set(dest)
    for sku in extra:
        if sku and sku not in seen:
            dest.append(sku)
            seen.add(sku)


def product_gid(product_id: str | int) -> str:
    pid = str(product_id).strip()
    if pid.startswith("gid://"):
        return pid
    return f"gid://shopify/Product/{pid}"


def lookup_product_id(sku: str, sku_to_product_id: dict[str, str]) -> str:
    if not sku or not sku_to_product_id:
        return ""
    for key in (sku, sku.upper(), sku.lower()):
        pid = sku_to_product_id.get(key)
        if pid:
            return str(pid)
    return ""


def resolve_skus_to_product_gids(
    skus: list[str],
    sku_to_product_id: dict[str, str],
    *,
    max_products: int = DEFAULT_MAX_PRODUCTS,
    exclude_product_ids: set[str] | None = None,
) -> tuple[list[str], int]:
    """
    Map SKUs → unique product GIDs (XML order), capped at max_products.
    Returns (gids, unresolved_sku_count).
    """
    exclude = exclude_product_ids or set()
    gids: list[str] = []
    seen_pids: set[str] = set()
    unresolved = 0
    for sku in skus:
        pid = lookup_product_id(sku, sku_to_product_id)
        if not pid:
            unresolved += 1
            continue
        if pid in exclude or pid in seen_pids:
            continue
        seen_pids.add(pid)
        gids.append(product_gid(pid))
        if len(gids) >= max_products:
            break
    return gids, unresolved


def stream_beziehung_relations(xml_path: str) -> dict[str, OwnerRelations]:
    """
    One CBEXPDN XML pass: owner PRODUKT → MUSS / EMPF / ET child SKUs.

    Nested PRODUKT under BEZIEHUNGSTYP must not be cleared before the parent ends.
    """
    out: dict[str, OwnerRelations] = {}
    context = etree.iterparse(xml_path, events=("end",), tag="PRODUKT")
    for _event, elem in context:
        try:
            parent = elem.getparent()
            if parent is not None and parent.tag == "BEZIEHUNGSTYP":
                continue
            bez = elem.find("BEZIEHUNGEN")
            if bez is None:
                continue
            owner = (elem.get("name") or "").strip()
            if not owner:
                continue
            muss: list[str] = []
            empf: list[str] = []
            et: list[str] = []
            for bt in bez.findall("BEZIEHUNGSTYP"):
                name = bt.get("name") or ""
                if name not in RELATION_TYPES:
                    continue
                kids = [
                    (c.get("name") or "").strip()
                    for c in bt.findall("PRODUKT")
                    if (c.get("name") or "").strip()
                ]
                if not kids:
                    continue
                if name == RELATION_MUSS:
                    _extend_unique(muss, kids)
                elif name == RELATION_EMPF:
                    _extend_unique(empf, kids)
                elif name == RELATION_ET:
                    _extend_unique(et, kids)
            if not (muss or empf or et):
                continue
            rel = out.get(owner)
            if rel is None:
                rel = OwnerRelations()
                out[owner] = rel
            _extend_unique(rel.muss, muss)
            _extend_unique(rel.empf, empf)
            _extend_unique(rel.et, et)
        finally:
            parent = elem.getparent()
            if parent is None or parent.tag != "BEZIEHUNGSTYP":
                elem.clear()
    return out


def merge_owner_relations(
    *maps: dict[str, OwnerRelations],
) -> dict[str, OwnerRelations]:
    merged: dict[str, OwnerRelations] = {}
    for m in maps:
        for owner, rel in m.items():
            cur = merged.get(owner)
            if cur is None:
                cur = OwnerRelations()
                merged[owner] = cur
            _extend_unique(cur.muss, rel.muss)
            _extend_unique(cur.empf, rel.empf)
            _extend_unique(cur.et, rel.et)
    return merged


def symmetrize_complementary_cliques(
    relations: dict[str, OwnerRelations],
) -> dict[str, OwnerRelations]:
    """
    Make complementary mutual within each original EMPF/MUSS group.

    For every owner O with complementary set S, every member of {O}∪S also
    gets the other members of that group. Overlapping groups are unioned per SKU
    (so a shared article can belong to multiple sets), but groups are not merged
    into one giant connected component via shared neighbors.
    """
    # sku → ordered complementary candidates from all groups it belongs to
    accrued: dict[str, list[str]] = defaultdict(list)

    for owner, rel in relations.items():
        group = _unique_preserve([owner] + rel.complementary_skus())
        if len(group) < 2:
            continue
        for member in group:
            others = [s for s in group if s != member]
            _extend_unique(accrued[member], others)

    if not accrued:
        return relations

    out: dict[str, OwnerRelations] = {
        owner: OwnerRelations(muss=list(rel.muss), empf=list(rel.empf), et=list(rel.et))
        for owner, rel in relations.items()
    }

    for sku, others in accrued.items():
        rel = out.get(sku) or OwnerRelations()
        ordered = _unique_preserve(rel.complementary_skus() + others)
        muss_set = set(rel.muss)
        muss_new = [s for s in ordered if s in muss_set]
        empf_new = [s for s in ordered if s not in muss_set]
        out[sku] = OwnerRelations(muss=muss_new, empf=empf_new, et=list(rel.et))

    return out


def build_relation_maps(
    xml_paths: list[str] | None = None,
    *,
    symmetrize_complementary: bool = True,
) -> dict[str, OwnerRelations]:
    paths = xml_paths if xml_paths is not None else resolve_cross_brand_xml_paths()
    maps = [stream_beziehung_relations(p) for p in paths]
    merged = merge_owner_relations(*maps) if maps else {}
    if symmetrize_complementary:
        return symmetrize_complementary_cliques(merged)
    return merged


def build_sync_targets(
    relations: dict[str, OwnerRelations],
    sku_to_product_id: dict[str, str],
    *,
    max_products: int = DEFAULT_MAX_PRODUCTS,
    only_skus: set[str] | None = None,
) -> dict[str, dict]:
    """
    Per owner SKU that exists in Shopify, compute complementary + ET product GID lists.

    Returns dict owner_sku → {
      owner_product_id, complementary_gids, et_gids,
      complementary_skus_raw, et_skus_raw,
      unresolved_complementary, unresolved_et,
    }
    Only includes owners with at least one of complementary/et source lists.
    """
    only_upper = {s.strip().upper() for s in only_skus} if only_skus else None
    targets: dict[str, dict] = {}

    for owner, rel in relations.items():
        if only_upper is not None and owner.strip().upper() not in only_upper:
            continue
        if not rel.has_complementary() and not rel.has_et():
            continue
        owner_pid = lookup_product_id(owner, sku_to_product_id)
        if not owner_pid:
            continue

        comp_skus = rel.complementary_skus()
        et_skus = _unique_preserve(rel.et)
        exclude = {owner_pid}

        comp_gids, unres_c = resolve_skus_to_product_gids(
            comp_skus,
            sku_to_product_id,
            max_products=max_products,
            exclude_product_ids=exclude,
        )
        et_gids, unres_e = resolve_skus_to_product_gids(
            et_skus,
            sku_to_product_id,
            max_products=max_products,
            exclude_product_ids=exclude,
        )

        if not comp_gids and not et_gids and not (rel.has_complementary() or rel.has_et()):
            continue

        # Keep owner if it has source relations (even if all recommended SKUs missing),
        # so dry-run can report empty resolution; push script skips empty writes.
        if not rel.has_complementary() and not rel.has_et():
            continue

        targets[owner] = {
            "owner_product_id": owner_pid,
            "owner_gid": product_gid(owner_pid),
            "complementary_gids": comp_gids if rel.has_complementary() else [],
            "et_gids": et_gids if rel.has_et() else [],
            "complementary_skus_raw": comp_skus,
            "et_skus_raw": et_skus,
            "has_complementary_source": rel.has_complementary(),
            "has_et_source": rel.has_et(),
            "unresolved_complementary": unres_c,
            "unresolved_et": unres_e,
        }
    return targets


def metafield_list_value(gids: list[str]) -> str:
    """JSON array string for list.product_reference metafieldsSet."""
    import json

    return json.dumps(gids, separators=(",", ":"))
