"""
Shopify Admin GraphQL → Supabase tabellen shopify_products / shopify_variants (+ optioneel ETA / YMM-metafields).

Vereist: config (of env) met SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN, SHOPIFY_ADMIN_API_VERSION.

Optioneel (env):
  SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE / SHOPIFY_VARIANT_ETA_METAFIELD_KEY — variant-ETA → shopify_eta
  SHOPIFY_PRODUCT_FITS_ON_NAMESPACE / SHOPIFY_PRODUCT_FITS_ON_KEY — product JSON → shopify_ymm.ymm_json
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

import requests

import config

_REQUEST_TIMEOUT = (15, 120)
_PAGE_PRODUCTS = 40
_PAGE_VARIANTS = 250


def _fits_on_ns_key() -> tuple[str, str]:
    ns = os.environ.get("SHOPIFY_PRODUCT_FITS_ON_NAMESPACE", "").strip()
    key = os.environ.get("SHOPIFY_PRODUCT_FITS_ON_KEY", "").strip()
    return (ns, key)


def _mirror_queries() -> tuple[str, str, bool, bool]:
    """(query_products, query_variant_page, use_fits_meta, use_eta_meta)."""
    fits_ns, fits_key = _fits_on_ns_key()
    use_fits = bool(fits_ns and fits_key)
    eta_ns = os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE", "").strip()
    eta_key = os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_KEY", "").strip()
    use_eta = bool(eta_ns and eta_key)

    fits_block = ""
    if use_fits:
        fits_block = f"""
        fitsOnMeta: metafield(namespace: \"{fits_ns}\", key: \"{fits_key}\") {{ value }}"""

    eta_block = ""
    if use_eta:
        eta_block = f"""
              etaMeta: metafield(namespace: \"{eta_ns}\", key: \"{eta_key}\") {{ value }}"""

    q_products = f"""
query MirrorProducts($cursor: String) {{
  products(first: {_PAGE_PRODUCTS}, after: $cursor) {{
    pageInfo {{ hasNextPage endCursor }}
    edges {{
      node {{
        id
        legacyResourceId
        handle
        title
        status
        publishedAt
        updatedAt{fits_block}
        variants(first: {_PAGE_VARIANTS}) {{
          pageInfo {{ hasNextPage endCursor }}
          edges {{
            node {{
              id
              legacyResourceId
              sku
              title
              price
              compareAtPrice
              updatedAt{eta_block}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

    q_variants = f"""
query MirrorVariants($id: ID!, $cursor: String) {{
  product(id: $id) {{
    variants(first: {_PAGE_VARIANTS}, after: $cursor) {{
      pageInfo {{ hasNextPage endCursor }}
      edges {{
        node {{
          id
          legacyResourceId
          sku
          title
          price
          compareAtPrice
          updatedAt{eta_block}
        }}
      }}
    }}
  }}
}}
"""
    return (q_products.strip(), q_variants.strip(), use_fits, use_eta)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.proxies = {"http": None, "https": None}
    return s


def _graphql_url() -> str:
    shop = config.SHOPIFY_SHOP_DOMAIN.strip()
    ver = config.SHOPIFY_ADMIN_API_VERSION.strip()
    return f"https://{shop}/admin/api/{ver}/graphql.json"


def _graphql(
    sess: requests.Session,
    query: str,
    variables: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    for attempt in range(25):
        r = sess.post(
            _graphql_url(),
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        body: dict[str, Any] = r.json()
        errs = body.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        if errs:
            raise RuntimeError(json.dumps(errs, ensure_ascii=False)[:2000])
        return body
    raise RuntimeError("GraphQL: te veel THROTTLED-pogingen")


def _dec_price(raw: object | None) -> str | None:
    """Shopify GraphQL: `price` is soms een Money-scalar (string), soms een object met `amount`."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return _dec_price(raw.get("amount"))
    s = str(raw).strip()
    if not s:
        return None
    try:
        return str(Decimal(s.replace(",", ".")))
    except InvalidOperation:
        return None


def _parse_eta_value(raw: str | None) -> tuple[str | None, str | None]:
    """Returns (iso_date or None, raw preserved)."""
    if not raw or not str(raw).strip():
        return (None, None)
    s = str(raw).strip()
    # YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return (s, s)
    return (None, s)


def _collect_variant_nodes(
    sess: requests.Session,
    product_gid: str,
    first_page: dict[str, Any],
    q_variants: str,
) -> list[dict[str, Any]]:
    """Eerste pagina zit in product node; haal rest op indien nodig."""
    vroot = first_page.get("variants") or {}
    edges = [e.get("node") or {} for e in (vroot.get("edges") or [])]
    out = [n for n in edges if n.get("legacyResourceId")]
    pi = vroot.get("pageInfo") or {}
    cursor = pi.get("endCursor")
    while pi.get("hasNextPage") and cursor:
        time.sleep(0.25)
        body = _graphql(sess, q_variants, {"id": product_gid, "cursor": cursor})
        pr = ((body.get("data") or {}).get("product") or {})
        vroot = pr.get("variants") or {}
        for e in vroot.get("edges") or []:
            n = e.get("node") or {}
            if n.get("legacyResourceId"):
                out.append(n)
        pi = vroot.get("pageInfo") or {}
        cursor = pi.get("endCursor")
    return out


def _supabase_upsert(
    sess: requests.Session,
    rest_base: str,
    headers: dict[str, str],
    table: str,
    rows: list[dict[str, Any]],
    on_conflict: str,
) -> None:
    if not rows:
        return
    h = {**headers, "Prefer": "resolution=merge-duplicates,return=minimal"}
    r = sess.post(
        f"{rest_base}/{table}",
        params={"on_conflict": on_conflict},
        headers=h,
        data=json.dumps(rows),
        timeout=_REQUEST_TIMEOUT,
    )
    if not r.ok:
        raise RuntimeError(f"Supabase {table} upsert {r.status_code}: {r.text[:1500]}")


def run_mirror(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """
    Haalt alle producten + varianten op en upsert naar Supabase.
    Returns (stats_dict, error_message).
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    if not config.SHOPIFY_ACCESS_TOKEN:
        return ({}, "SHOPIFY_ACCESS_TOKEN ontbreekt (zet in .env of GitHub Secrets).")

    stats: dict[str, Any] = {
        "products_upserted": 0,
        "variants_upserted": 0,
        "ymm_rows": 0,
        "eta_rows": 0,
        "pages": 0,
    }
    synced = _iso_now()
    shop_sess = _http_session()

    q_products, q_variants, use_fits, _use_eta = _mirror_queries()

    product_cursor: str | None = None

    try:
        while True:
            stats["pages"] += 1
            body = _graphql(shop_sess, q_products, {"cursor": product_cursor})
            conn = ((body.get("data") or {}).get("products") or {})
            page_info = conn.get("pageInfo") or {}
            edges = conn.get("edges") or []

            prod_rows: list[dict[str, Any]] = []
            var_rows: list[dict[str, Any]] = []
            ymm_rows: list[dict[str, Any]] = []
            eta_rows: list[dict[str, Any]] = []

            for edge in edges:
                p = edge.get("node") or {}
                pid = p.get("legacyResourceId")
                if not pid:
                    continue
                pid_int = int(pid)
                raw_p = {
                    "gid": p.get("id"),
                    "status": p.get("status"),
                }
                prod_rows.append(
                    {
                        "shopify_product_id": pid_int,
                        "handle": p.get("handle"),
                        "title": p.get("title"),
                        "status": (p.get("status") or "").upper() or None,
                        "published_at": p.get("publishedAt"),
                        "updated_at_shopify": p.get("updatedAt"),
                        "raw": raw_p,
                        "synced_at": synced,
                    }
                )

                if use_fits:
                    meta = p.get("fitsOnMeta")
                    val = (meta or {}).get("value") if meta else None
                    if val is not None and str(val).strip():
                        try:
                            parsed = json.loads(val) if isinstance(val, str) else val
                        except json.JSONDecodeError:
                            parsed = {"_raw": val}
                        ymm_rows.append(
                            {
                                "shopify_product_id": pid_int,
                                "ymm_json": parsed,
                                "synced_at": synced,
                            }
                        )

                vnodes = _collect_variant_nodes(
                    shop_sess, p.get("id") or "", p, q_variants
                )
                for v in vnodes:
                    vid = v.get("legacyResourceId")
                    if not vid:
                        continue
                    vid_int = int(vid)
                    raw_v = {"gid": v.get("id")}
                    var_rows.append(
                        {
                            "shopify_variant_id": vid_int,
                            "shopify_product_id": pid_int,
                            "sku": (v.get("sku") or "").strip() or None,
                            "title": v.get("title"),
                            "price": _dec_price(v.get("price")),
                            "compare_at_price": _dec_price(v.get("compareAtPrice")),
                            "updated_at_shopify": v.get("updatedAt"),
                            "raw": raw_v,
                            "synced_at": synced,
                        }
                    )
                    em = v.get("etaMeta")
                    ev = (em or {}).get("value") if em else None
                    if ev is not None and str(ev).strip():
                        d, raw_t = _parse_eta_value(str(ev).strip())
                        eta_rows.append(
                            {
                                "shopify_variant_id": vid_int,
                                "eta_date": d,
                                "eta_raw": raw_t,
                                "synced_at": synced,
                            }
                        )

            _supabase_upsert(
                supabase_sess,
                rest_base,
                supabase_headers,
                "shopify_products",
                prod_rows,
                "shopify_product_id",
            )
            _supabase_upsert(
                supabase_sess,
                rest_base,
                supabase_headers,
                "shopify_variants",
                var_rows,
                "shopify_variant_id",
            )
            if ymm_rows:
                _supabase_upsert(
                    supabase_sess,
                    rest_base,
                    supabase_headers,
                    "shopify_ymm",
                    ymm_rows,
                    "shopify_product_id",
                )
            if eta_rows:
                _supabase_upsert(
                    supabase_sess,
                    rest_base,
                    supabase_headers,
                    "shopify_eta",
                    eta_rows,
                    "shopify_variant_id",
                )

            stats["products_upserted"] += len(prod_rows)
            stats["variants_upserted"] += len(var_rows)
            stats["ymm_rows"] += len(ymm_rows)
            stats["eta_rows"] += len(eta_rows)

            if not page_info.get("hasNextPage"):
                break
            product_cursor = page_info.get("endCursor")
            time.sleep(0.35)

        parts = [
            f"Spiegel klaar: {stats['products_upserted']} producten, "
            f"{stats['variants_upserted']} varianten, {stats['pages']} Shopify-pagina's."
        ]
        if stats["ymm_rows"]:
            parts.append(f"YMM: {stats['ymm_rows']} rijen.")
        if stats["eta_rows"]:
            parts.append(f"ETA: {stats['eta_rows']} rijen.")
        _log(" ".join(parts))

        return (stats, None)
    except (requests.RequestException, RuntimeError, ValueError, KeyError) as e:
        return (stats, str(e)[:4000])
