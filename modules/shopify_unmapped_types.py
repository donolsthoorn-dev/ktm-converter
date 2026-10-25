"""Producttypes zonder positieve slimme-collectie TYPE-regel.

Alleen types met ≥1 ACTIVE product dat op Online Store staat
(`published_status:published`) — POS-only kopieën tellen niet mee.
"""

from __future__ import annotations

import re
import time

from modules.shopify_collections import fetch_all_collections, graphql_post, http_session

_POSITIVE = frozenset({"EQUALS", "CONTAINS", "STARTS_WITH", "ENDS_WITH"})
_OEM_TYPE_RE = re.compile(r"^(KTM|HSQ|WP)\s*[-–]", re.I)

_PRODUCT_TYPES_QUERY = """
query ProductTypesPage($cursor: String) {
  productTypes(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges { node }
  }
}
"""

_PRODUCTS_COUNT = """
query ProductsCountForType($q: String!) {
  productsCount(query: $q) { count }
}
"""


def _is_oem_type(t: str) -> bool:
    s = (t or "").strip()
    if not s:
        return False
    return bool(_OEM_TYPE_RE.match(s))


def _type_matches_rule(product_type: str, relation: str, condition: str) -> bool:
    t = (product_type or "").strip()
    c = (condition or "").strip()
    if not t:
        return False
    rel = (relation or "").strip().upper()
    if rel == "EQUALS":
        return t == c
    if rel == "CONTAINS":
        return c.casefold() in t.casefold()
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
                yield r


def _escape_query_value(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _fetch_product_types(shop: str, token: str, api_version: str) -> list[str]:
    sess = http_session()
    types: list[str] = []
    cursor: str | None = None
    while True:
        data = graphql_post(
            shop,
            token,
            api_version,
            _PRODUCT_TYPES_QUERY,
            {"cursor": cursor},
            sess=sess,
        )
        conn = (data.get("data") or {}).get("productTypes") or {}
        for edge in conn.get("edges") or []:
            t = (edge.get("node") or "").strip()
            if t:
                types.append(t)
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if not cursor:
            break
    return sorted(set(types))


def _has_online_store_products(
    shop: str,
    token: str,
    api_version: str,
    product_type: str,
    *,
    sess=None,
) -> bool:
    """True als er ≥1 ACTIVE + Online Store-gepubliceerd product van dit type is."""
    sess = sess or http_session()
    q = (
        f'product_type:"{_escape_query_value(product_type)}" '
        f"status:active published_status:published"
    )
    data = graphql_post(
        shop,
        token,
        api_version,
        _PRODUCTS_COUNT,
        {"q": q},
        sess=sess,
    )
    count = ((data.get("data") or {}).get("productsCount") or {}).get("count") or 0
    return int(count) > 0


def type_covered_by_rules(product_type: str, type_rules: list[dict]) -> bool:
    for r in type_rules:
        rel = (r.get("relation") or "").upper()
        if rel not in _POSITIVE:
            continue
        if _type_matches_rule(product_type, rel, r.get("condition") or ""):
            return True
    return False


def unmapped_active_types(
    shop: str,
    token: str,
    api_version: str,
    *,
    exclude_oem: bool = False,
    sleep_s: float = 0.05,
) -> list[str]:
    """Types met ≥1 Online Store-product die geen positieve TYPE-regel matchen."""
    collections = fetch_all_collections(shop, token, api_version)
    type_rules = list(_iter_type_rules(collections))
    all_types = _fetch_product_types(shop, token, api_version)

    candidates: list[str] = []
    for t in all_types:
        if not t:
            continue
        if exclude_oem and _is_oem_type(t):
            continue
        if type_covered_by_rules(t, type_rules):
            continue
        candidates.append(t)

    sess = http_session()
    out: list[str] = []
    for i, t in enumerate(candidates):
        if _has_online_store_products(shop, token, api_version, t, sess=sess):
            out.append(t)
        if sleep_s and i + 1 < len(candidates):
            time.sleep(sleep_s)
    return out
