#!/usr/bin/env python3
"""Eenmalige analyse: Motox producttypes vs smart-collection TYPE-regels.

Sluit KTM/HSQ/WP-types uit. Schrijft JSON naar output/.
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.motox_shopify_cache import _gql, _session, load_motox_env  # noqa: E402
from modules.shopify_collections import fetch_all_collections  # noqa: E402

OUT = PROJECT_ROOT / "output" / "motox_unmapped_types.json"

_POSITIVE = frozenset({"EQUALS", "CONTAINS", "STARTS_WITH", "ENDS_WITH"})
_OEM_TYPE_RE = re.compile(r"^(KTM|HSQ|WP)\s*[-–]", re.I)
_OEM_VENDORS = {
    "ktm",
    "husqvarna",
    "husqvarna motorcycles",
    "wp",
    "wp suspension",
    "gasgas",
    "gas gas",
}

_PRODUCT_TYPES_QUERY = """
query ProductTypesPage($cursor: String) {
  productTypes(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges { node }
  }
}
"""

_PRODUCTS_FOR_TYPE = """
query ProductsForType($q: String!) {
  productsCount(query: $q) { count }
  products(first: 8, query: $q) {
    nodes {
      title
      handle
      vendor
      status
      productType
      collections(first: 12) {
        nodes { title handle }
      }
    }
  }
}
"""


def _is_oem_type(t: str) -> bool:
    s = (t or "").strip()
    if not s:
        return False
    if _OEM_TYPE_RE.match(s):
        return True
    # PowerParts-stijl Engelse types komen ook voor op gekloonde KTM-producten
    # maar die zijn niet eenduidig. Alleen prefix-filter hier.
    return False


def _type_matches_rule(product_type: str, relation: str, condition: str) -> bool:
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


def _fetch_types(sess, url, token) -> list[str]:
    types: list[str] = []
    cursor = None
    while True:
        body = _gql(
            sess, url, token, _PRODUCT_TYPES_QUERY, {"cursor": cursor}
        )
        conn = (body.get("data") or {}).get("productTypes") or {}
        for edge in conn.get("edges") or []:
            t = (edge.get("node") or "").strip()
            if t:
                types.append(t)
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
    return sorted(set(types))


def _escape_query_value(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def main() -> int:
    domain, token, api = load_motox_env()
    url = f"https://{domain}/admin/api/{api}/graphql.json"
    print(f"Shop: {domain}  API {api}", flush=True)

    print("Collecties ophalen…", flush=True)
    collections = fetch_all_collections(domain, token, api)
    print(f"  {len(collections)} collecties", flush=True)

    sess = _session()
    print("Producttypes ophalen…", flush=True)
    all_types = _fetch_types(sess, url, token)
    print(f"  {len(all_types)} unieke types", flush=True)

    oem_types = [t for t in all_types if _is_oem_type(t)]
    motox_types = [t for t in all_types if not _is_oem_type(t)]

    type_rules = []
    equals_values = set()
    for c, r in _iter_type_rules(collections):
        cond = (r.get("condition") or "").strip()
        rel = (r.get("relation") or "").upper()
        type_rules.append(
            {
                "collection": c.get("title"),
                "handle": c.get("handle"),
                "relation": rel,
                "condition": cond,
                "appliedDisjunctively": (c.get("ruleSet") or {}).get(
                    "appliedDisjunctively"
                ),
            }
        )
        if rel == "EQUALS" and cond:
            equals_values.add(cond)

    smart = []
    manual = []
    for c in collections:
        rs = c.get("ruleSet") or {}
        rules = rs.get("rules") or []
        item = {
            "title": c.get("title"),
            "handle": c.get("handle"),
            "kind": "smart" if rules else "manual",
            "appliedDisjunctively": rs.get("appliedDisjunctively"),
            "rules": [
                {
                    "column": x.get("column"),
                    "relation": x.get("relation"),
                    "condition": x.get("condition"),
                }
                for x in rules
            ],
        }
        (smart if rules else manual).append(item)

    unmatched_equals = [t for t in motox_types if t not in equals_values]
    unmatched_positive = []
    type_to_collections: dict[str, list[str]] = {}
    for tv in motox_types:
        hits = []
        for c, r in _iter_type_rules(collections):
            rel = (r.get("relation") or "").upper()
            if rel not in _POSITIVE:
                continue
            if _type_matches_rule(tv, rel, r.get("condition") or ""):
                title = c.get("title") or ""
                if title not in hits:
                    hits.append(title)
        type_to_collections[tv] = hits
        if not hits:
            unmatched_positive.append(tv)

    print(
        f"Motox-types (ex OEM): {len(motox_types)}  "
        f"geen EQUALS: {len(unmatched_equals)}  "
        f"geen positieve TYPE-regel: {len(unmatched_positive)}",
        flush=True,
    )

    details = []
    # Alle types zonder positieve TYPE-match + types met '>' (e-bihr paden)
    # ook als ze via CONTAINS toch matchen — voor banden-benaming.
    ebihr_path_types = [t for t in motox_types if " > " in t]
    inspect = sorted(set(unmatched_positive) | set(ebihr_path_types))
    print(f"Details ophalen voor {len(inspect)} types…", flush=True)

    for i, tv in enumerate(inspect, 1):
        q = f'product_type:"{_escape_query_value(tv)}"'
        body = _gql(sess, url, token, _PRODUCTS_FOR_TYPE, {"q": q})
        data = body.get("data") or {}
        count = ((data.get("productsCount") or {}).get("count")) or 0
        nodes = ((data.get("products") or {}).get("nodes")) or []
        samples = []
        collection_titles = set()
        oem_vendor_n = 0
        for n in nodes:
            vendor = (n.get("vendor") or "").strip()
            if vendor.casefold() in _OEM_VENDORS:
                oem_vendor_n += 1
            cols = [
                x.get("title")
                for x in ((n.get("collections") or {}).get("nodes") or [])
                if x.get("title")
            ]
            collection_titles.update(cols)
            samples.append(
                {
                    "title": n.get("title"),
                    "handle": n.get("handle"),
                    "vendor": vendor,
                    "status": n.get("status"),
                    "collections": cols,
                }
            )
        details.append(
            {
                "type": tv,
                "count": count,
                "matched_type_collections": type_to_collections.get(tv, []),
                "sample_product_collections": sorted(collection_titles),
                "sample_oem_vendor": oem_vendor_n,
                "samples": samples,
                "is_ebihr_path": " > " in tv,
                "unmatched_positive": tv in unmatched_positive,
            }
        )
        if i % 25 == 0:
            print(f"  {i}/{len(inspect)}", flush=True)
        time.sleep(0.15)

    payload = {
        "shop": domain,
        "api": api,
        "collection_count": len(collections),
        "smart_count": len(smart),
        "manual_count": len(manual),
        "all_type_count": len(all_types),
        "oem_type_count": len(oem_types),
        "motox_type_count": len(motox_types),
        "unmatched_equals_count": len(unmatched_equals),
        "unmatched_positive_count": len(unmatched_positive),
        "oem_types": oem_types,
        "motox_types": motox_types,
        "unmatched_equals": unmatched_equals,
        "unmatched_positive": unmatched_positive,
        "type_rules": type_rules,
        "smart_collections": smart,
        "manual_collections": [{"title": m["title"], "handle": m["handle"]} for m in manual],
        "details": details,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Geschreven: {OUT}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
