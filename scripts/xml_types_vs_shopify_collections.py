#!/usr/bin/env python3
"""
Vergelijkt **Type**-waarden uit de KTM XML (zoals `load_products()` / export-kolom Type)
met smart-collection **TYPE**-regels in Shopify.

Shopify kan producten in collecties plaatsen via TAG, TITLE, prijs, voorraad, enz. Dit script
kijkt daarom alleen naar **Product type**-regels (`column == TYPE`):

1. **Exact EQUALS** — XML-types die nergens als `TYPE EQUALS "<waarde>"` voorkomen.
2. **Positieve TYPE-regels** — types die door geen enkele regel met relation EQUALS / CONTAINS /
   STARTS_WITH / ENDS_WITH op het producttype matchen (zoals in de Admin bedoeld).

Let op: een type kan alsnog in collecties vallen via TAG/TITLE/prijs/voorraad. Zie de uitleg
in de output.

  python3 scripts/xml_types_vs_shopify_collections.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from modules.shopify_collections import fetch_all_collections
from modules.xml_loader import load_products

# Zelfde relation-waarden als Shopify GraphQL (uppercase enum)
_POSITIVE = frozenset({"EQUALS", "CONTAINS", "STARTS_WITH", "ENDS_WITH"})


def _type_matches_rule(product_type: str, relation: str, condition: str) -> bool:
    """Of een producttype-string voldoet aan één TYPE-regel (Shopify-semantiek, vereenvoudigd)."""
    t = (product_type or "").strip()
    c = (condition or "").strip()
    if not t:
        return False
    rel = (relation or "").strip().upper()
    if rel == "EQUALS":
        return t == c
    if rel == "NOT_EQUALS":
        return t != c
    if rel == "CONTAINS":
        return c.casefold() in t.casefold()
    if rel == "NOT_CONTAINS":
        return c.casefold() not in t.casefold()
    if rel == "STARTS_WITH":
        return t.casefold().startswith(c.casefold())
    if rel == "ENDS_WITH":
        return t.casefold().endswith(c.casefold())
    return False


def _iter_type_rules(collections: list[dict]):
    for c in collections:
        rs = c.get("ruleSet") or {}
        for r in rs.get("rules") or []:
            if r.get("column") == "TYPE":
                yield c, r


def main() -> int:
    p = argparse.ArgumentParser(
        description="XML Type-waarden vs. Shopify TYPE-collectieregels"
    )
    p.add_argument(
        "--include-excluded-types",
        action="store_true",
        help="Ook types die in config.DELTA_EXCLUDED_TYPES zitten (default: zelfde filter als export)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Compact JSON naar stdout",
    )
    args = p.parse_args()

    token = (config.SHOPIFY_ACCESS_TOKEN or "").strip()
    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt — zie .env", file=sys.stderr)
        return 1

    print("XML laden…", flush=True)
    products = load_products()
    xml_types: set[str] = set()
    excluded = config.DELTA_EXCLUDED_TYPES
    for pdict in products:
        tv = (pdict.get("type") or "").strip()
        if not tv:
            continue
        if not args.include_excluded_types and tv in excluded:
            continue
        xml_types.add(tv)

    print("Shopify-collecties ophalen…", flush=True)
    try:
        collections = fetch_all_collections(
            config.SHOPIFY_SHOP_DOMAIN.strip(),
            token,
            config.SHOPIFY_ADMIN_API_VERSION.strip(),
        )
    except Exception as e:
        print(e, file=sys.stderr)
        return 1

    equals_values: set[str] = set()
    for _c, r in _iter_type_rules(collections):
        if (r.get("relation") or "").upper() == "EQUALS":
            cond = (r.get("condition") or "").strip()
            if cond:
                equals_values.add(cond)

    not_in_any_equals = sorted(xml_types - equals_values)

    without_positive_match: list[str] = []
    for tv in sorted(xml_types):
        matched = False
        for _c, r in _iter_type_rules(collections):
            rel = (r.get("relation") or "").upper()
            if rel not in _POSITIVE:
                continue
            if _type_matches_rule(tv, r.get("relation") or "", r.get("condition") or ""):
                matched = True
                break
        if not matched:
            without_positive_match.append(tv)

    out = {
        "xml_type_count": len(xml_types),
        "shopify_collections": len(collections),
        "type_equals_conditions_in_shopify": len(equals_values),
        "xml_types_not_listed_as_type_equals": not_in_any_equals,
        "xml_types_without_positive_type_rule": without_positive_match,
    }

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    shop = config.SHOPIFY_SHOP_DOMAIN.strip()
    ver = config.SHOPIFY_ADMIN_API_VERSION.strip()
    print(f"\nShop: {shop}  (API {ver})")
    print(f"Unieke Type-waarden uit XML (na filter): {len(xml_types)}")
    print(
        "\n--- 1) Types die nergens als TYPE EQUALS \"…\" in een smart collection voorkomen ---\n"
        "    (exacte string zoals in Shopify Admin; geen CONTAINS/TAG/TITLE.)\n"
    )
    if not not_in_any_equals:
        print("    (geen — alle XML-types komen minstens ergens voor als EQUALS-waarde)\n")
    else:
        for t in not_in_any_equals:
            print(f"    • {t}")
        print()

    print(
        "--- 2) Types die door geen enkele positieve TYPE-regel worden geraakt ---\n"
        "    (EQUALS / CONTAINS / STARTS_WITH / ENDS_WITH op kolom TYPE.)\n"
        "    Producten met dit type kunnen nog wél in collecties zitten via TAG, TITLE,\n"
        "    prijs, voorraad, of handmatige collectie.\n"
    )
    if not without_positive_match:
        print("    (geen — elk type matcht minstens één positieve TYPE-regel)\n")
    else:
        for t in without_positive_match:
            print(f"    • {t}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
