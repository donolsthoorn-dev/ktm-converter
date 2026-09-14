#!/usr/bin/env python3
"""
Vergelijkt **Type**-waarden (XML of live Shopify) met smart-collection **TYPE**-regels.

Ondersteunt merken KTM, HSQ en WP (`--brand`). Bij `--source shopify` worden unieke
producttypes via GraphQL opgehaald (snel); HSQ/WP worden herkend aan het prefix
``HSQ - `` / ``WP - `` op het type.

Shopify kan producten in collecties plaatsen via TAG, TITLE, prijs, voorraad, enz. Dit script
kijkt daarom alleen naar **Product type**-regels (`column == TYPE`):

1. **Exact EQUALS** — types die nergens als `TYPE EQUALS "<waarde>"` voorkomen.
2. **Positieve TYPE-regels** — types die door geen enkele regel met relation EQUALS / CONTAINS /
   STARTS_WITH / ENDS_WITH op het producttype matchen (zoals in de Admin bedoeld).

Let op: een type kan alsnog in collecties vallen via TAG/TITLE/prijs/voorraad. Zie de uitleg
in de output.

  python3 scripts/xml_types_vs_shopify_collections.py --source shopify
  python3 scripts/xml_types_vs_shopify_collections.py --source shopify --brand hsq
  python3 scripts/xml_types_vs_shopify_collections.py --brand wp
  python3 scripts/xml_types_vs_shopify_collections.py --brand all --source shopify
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
from modules.brand_config import VALID_BRAND_IDS, get_brand_config
from modules.shopify_collections import fetch_all_collections, graphql_post
from modules.xml_loader import load_products

# Zelfde relation-waarden als Shopify GraphQL (uppercase enum)
_POSITIVE = frozenset({"EQUALS", "CONTAINS", "STARTS_WITH", "ENDS_WITH"})

_PRODUCT_TYPES_QUERY = """
query ProductTypesPage($cursor: String) {
  productTypes(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges { node }
  }
}
"""


def _fetch_shopify_product_types(shop: str, token: str, api_version: str) -> set[str]:
    """Haal unieke product_type-waarden op via GraphQL productTypes (snel)."""
    types: set[str] = set()
    cursor: str | None = None
    while True:
        body = graphql_post(
            shop,
            token,
            api_version,
            _PRODUCT_TYPES_QUERY,
            {"cursor": cursor},
        )
        conn = (body.get("data") or {}).get("productTypes") or {}
        for edge in conn.get("edges") or []:
            t = (edge.get("node") or "").strip()
            if t:
                types.add(t)
        page_info = conn.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
    return types


def _filter_types_for_brand(type_values: set[str], brand_id: str) -> set[str]:
    """Beperk Shopify-types tot één merk (KTM = geen HSQ/WP-prefix)."""
    brand = get_brand_config(brand_id)
    prefix = brand.shopify_type_tag_prefix
    if prefix:
        return {t for t in type_values if t.startswith(prefix)}
    return {
        t
        for t in type_values
        if not t.startswith("HSQ - ") and not t.startswith("WP - ")
    }


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


def _analyze_types(
    type_values: set[str],
    collections: list[dict],
) -> tuple[list[str], list[str]]:
    equals_values: set[str] = set()
    for _c, r in _iter_type_rules(collections):
        if (r.get("relation") or "").upper() == "EQUALS":
            cond = (r.get("condition") or "").strip()
            if cond:
                equals_values.add(cond)

    not_in_any_equals = sorted(type_values - equals_values)

    without_positive_match: list[str] = []
    for tv in sorted(type_values):
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

    return not_in_any_equals, without_positive_match


def _load_type_values(
    source: str,
    brand_id: str,
    include_excluded_types: bool,
) -> tuple[set[str], str]:
    type_values: set[str] = set()
    source_label = "XML"
    if source == "xml":
        config.apply_brand(brand_id)
        print(f"XML laden ({brand_id})…", flush=True)
        products = load_products()
        excluded = config.DELTA_EXCLUDED_TYPES
        prefix = get_brand_config(brand_id).shopify_type_tag_prefix
        for pdict in products:
            tv = (pdict.get("type") or "").strip()
            if not tv:
                continue
            if prefix and not tv.startswith(prefix):
                tv = f"{prefix}{tv}"
            if not include_excluded_types and tv in excluded:
                continue
            type_values.add(tv)
    else:
        source_label = "Shopify-producttypes"
        print("Shopify-producttypes laden…", flush=True)
        all_types = _fetch_shopify_product_types(
            config.SHOPIFY_SHOP_DOMAIN.strip(),
            (config.SHOPIFY_ACCESS_TOKEN or "").strip(),
            config.SHOPIFY_ADMIN_API_VERSION.strip(),
        )
        type_values = _filter_types_for_brand(all_types, brand_id)
    return type_values, source_label


def _print_brand_report(
    brand_id: str,
    source_label: str,
    type_values: set[str],
    collections: list[dict],
    not_in_any_equals: list[str],
    without_positive_match: list[str],
) -> None:
    shop = config.SHOPIFY_SHOP_DOMAIN.strip()
    ver = config.SHOPIFY_ADMIN_API_VERSION.strip()
    print(f"\n{'=' * 60}")
    print(f"Merk: {brand_id.upper()}  |  Shop: {shop}  (API {ver})")
    print(f"Unieke Type-waarden uit {source_label}: {len(type_values)}")
    print(
        "\n--- 1) Types die nergens als TYPE EQUALS \"…\" in een smart collection voorkomen ---\n"
        "    (exacte string zoals in Shopify Admin; geen CONTAINS/TAG/TITLE.)\n"
    )
    if not not_in_any_equals:
        print("    (geen — alle types komen minstens ergens voor als EQUALS-waarde)\n")
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


def main() -> int:
    p = argparse.ArgumentParser(
        description="Type-waarden (XML/Shopify) vs. Shopify TYPE-collectieregels"
    )
    p.add_argument(
        "--brand",
        metavar="ID",
        default=None,
        help=f"Merk: {', '.join(sorted(VALID_BRAND_IDS))}, of 'all' voor alle drie (default: env BRAND of ktm).",
    )
    p.add_argument(
        "--source",
        choices=("xml", "shopify"),
        default="shopify",
        help="Bron voor Type-waarden: live Shopify (default) of XML per merk.",
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

    raw_brand = (args.brand or config.BRAND_ID or "ktm").strip().lower()
    if raw_brand == "all":
        brand_ids = sorted(VALID_BRAND_IDS)
    else:
        if raw_brand not in VALID_BRAND_IDS:
            print(
                f"Onbekend merk '{args.brand}'. Kies: {', '.join(sorted(VALID_BRAND_IDS))}, all.",
                file=sys.stderr,
            )
            return 1
        brand_ids = [raw_brand]

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

    reports: list[dict] = []
    for brand_id in brand_ids:
        try:
            type_values, source_label = _load_type_values(
                args.source,
                brand_id,
                args.include_excluded_types,
            )
        except Exception as e:
            print(e, file=sys.stderr)
            return 1

        not_in_any_equals, without_positive_match = _analyze_types(type_values, collections)
        reports.append(
            {
                "brand": brand_id,
                "type_source": args.source,
                "source_type_count": len(type_values),
                "types_not_listed_as_type_equals": not_in_any_equals,
                "types_without_positive_type_rule": without_positive_match,
            }
        )

        if not args.json:
            _print_brand_report(
                brand_id,
                source_label,
                type_values,
                collections,
                not_in_any_equals,
                without_positive_match,
            )

    if args.json:
        out = {
            "shopify_collections": len(collections),
            "brands": reports,
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
