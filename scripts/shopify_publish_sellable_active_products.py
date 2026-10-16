#!/usr/bin/env python3
"""
Publiceer ACTIVE Shopify-producten op álle verkoopkanalen (Webshop, Shop, Facebook,
POS, Google, …) wanneer ze dat volgens ERP zouden moeten zijn.

Kandidaten:
  - status ACTIVE + ERP-verkoopbaar + type/afbeelding OK, én
  - published_at leeg (nog niet op Webshop), of
  - wel op Webshop maar nog ontbrekende publicatiekanalen (Shop/Facebook/…)

Niet publiceren / bij --apply depubliceren van alle kanalen:
  - uitgesloten type, leeg type, geen afbeelding, uitverkocht, of ERP StockAvailable=0

Mutatie: publishablePublish / publishableUnpublish op alle publications
(vereist read_publications + write_publications). Fallback: alleen Online Store
via productUpdate published true/false.

Standaard dry-run. Met --apply worden producten echt gepubliceerd.

Voorbeelden:
  python3 scripts/shopify_publish_sellable_active_products.py
  python3 scripts/shopify_publish_sellable_active_products.py --apply
  python3 scripts/shopify_publish_sellable_active_products.py --apply --limit 50
  # Na dry-run: apply zonder opnieuw 30k producten te scannen (~1 min i.p.v. ~70 min):
  python3 scripts/shopify_publish_sellable_active_products.py --apply --reuse-candidates
  python3 scripts/shopify_publish_sellable_active_products.py --input-csv output/active_not_on_webshop.csv

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN
Optioneel: ArticleStatus/StockAvailable uit alle merk-prijs-CSV's (KTM+HSQ+WP)
Optioneel: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (mirror published_at bijwerken na succes)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402
from modules.pricing_loader import (  # noqa: E402
    load_article_status_from_35_z1_csv_files,
    load_stock_available_from_35_z1_csv_files,
    lookup_in_str_index,
    normalize_sku_key,
)
from modules.shopify_product_availability import (  # noqa: E402
    product_unavailable_for_webshop,
    unavailable_reason_label,
)

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)

_GQL_PRODUCT_PUBLISHED = """
mutation KtmSetOnlineStorePublished($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      status
      publishedAt
    }
    userErrors {
      field
      message
    }
  }
}
"""

_GQL_PUBLICATIONS = """
query KtmPublications {
  publications(first: 50) {
    nodes { id name }
  }
}
"""

_GQL_PUBLISHABLE_PUBLISH = """
mutation KtmPublishablePublish($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

_GQL_PUBLISHABLE_UNPUBLISH = """
mutation KtmPublishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
  publishableUnpublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

_GQL_PRODUCTS_MISSING_CHANNELS = """
query KtmProductsMissingChannels($cursor: String) {
  products(first: 50, after: $cursor, query: "status:active") {
    pageInfo { hasNextPage endCursor }
    nodes {
      legacyResourceId
      handle
      title
      productType
      publishedAt
      featuredImage { url }
      variants(first: 100) {
        nodes { sku inventoryQuantity }
      }
      unpublishedPublications(first: 20) {
        nodes { id name }
      }
    }
  }
}
"""

_TYPE_PREFIXES = ("HSQ - ", "WP - ", "KTM - ")

# Cache: None = nog niet opgehaald; [] = geen scope / leeg.
_PUBLICATION_IDS: list[str] | None = None
_PUBLICATION_NAMES: dict[str, str] = {}


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _graphql_post(
    sess: requests.Session,
    query: str,
    variables: dict | None = None,
) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    last: dict = {}
    for attempt in range(25):
        r = sess.post(
            _GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": TOKEN,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        if r.status_code >= 500:
            time.sleep(min(3.0 * (attempt + 1), 30.0))
            continue
        r.raise_for_status()
        last = r.json()
        if last.get("errors"):
            return last
        return last
    return last


def _next_url_from_link_header(link: str | None) -> str | None:
    if not link:
        return None
    for part in link.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None


def _variant_article_statuses(
    variants: list[dict],
    status_by_sku: dict[str, str],
) -> list[str]:
    out: list[str] = []
    for v in variants:
        sku = normalize_sku_key(v.get("sku"))
        if not sku:
            continue
        st = lookup_in_str_index(status_by_sku, sku).strip()
        if st:
            out.append(st)
    return out


def _product_is_sellable_per_csv(
    variants: list[dict],
    status_by_sku: dict[str, str],
    *,
    stock_by_sku: dict[str, int] | None = None,
) -> bool:
    statuses = _variant_article_statuses(variants, status_by_sku)
    if not statuses:
        return False
    if not any(st != "80" for st in statuses):
        return False
    if product_unavailable_for_webshop(variants, stock_by_sku=stock_by_sku):
        return False
    return True


def _product_has_shopify_image(product: dict) -> bool:
    for img in product.get("images") or []:
        if (img.get("src") or "").strip():
            return True
    return False


def _type_matches_excluded_set(product_type: str) -> bool:
    raw = (product_type or "").strip()
    if not raw:
        return False
    if raw in config.DELTA_EXCLUDED_TYPES:
        return True
    for prefix in _TYPE_PREFIXES:
        if raw.startswith(prefix):
            suffix = raw[len(prefix) :].strip()
            if suffix in config.DELTA_EXCLUDED_TYPES:
                return True
    return False


def _online_store_exclusion_reason(
    product: dict,
    *,
    stock_by_sku: dict[str, int] | None = None,
) -> str | None:
    product_type = (product.get("product_type") or "").strip()
    if not product_type:
        return "geen_type"
    if _type_matches_excluded_set(product_type):
        return f"type:{product_type}"
    if not _product_has_shopify_image(product):
        return "geen_afbeelding"
    variants = product.get("variants") or []
    unavail = unavailable_reason_label(variants, stock_by_sku=stock_by_sku)
    if unavail:
        return unavail
    return None


def _row_from_product(p: dict, *, status_by_sku: dict[str, str] | None = None) -> dict:
    variants = p.get("variants") or []
    skus = [normalize_sku_key(v.get("sku")) for v in variants if v.get("sku")]
    statuses: list[str] = []
    if status_by_sku is not None:
        statuses = [
            lookup_in_str_index(status_by_sku, s).strip()
            for s in skus
        ]
        statuses = [s for s in statuses if s]
    return {
        "product_id": str(p.get("id") or ""),
        "handle": (p.get("handle") or "").strip(),
        "title": (p.get("title") or "").strip()[:120],
        "product_type": (p.get("product_type") or "").strip(),
        "skus": ",".join(skus[:5]) + ("..." if len(skus) > 5 else ""),
        "article_statuses": ",".join(sorted(set(statuses))),
        "variant_count": len(variants),
    }


def _collect_candidates_rest(
    sess: requests.Session,
    status_by_sku: dict[str, str],
    stock_by_sku: dict[str, int],
) -> tuple[list[dict], list[dict]]:
    """ACTIVE + published_at leeg + ERP-verkoopbaar (Webshop nog uit)."""
    headers = {"X-Shopify-Access-Token": TOKEN}
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        f"?limit=250&status=active"
        f"&fields=id,handle,title,status,published_at,product_type,images,variants"
    )
    scanned = 0
    candidates: list[dict] = []
    skipped: list[dict] = []

    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(2)
            continue
        if r.status_code >= 500:
            time.sleep(3)
            continue
        r.raise_for_status()

        for p in r.json().get("products", []):
            scanned += 1
            if (p.get("published_at") or "").strip():
                continue
            variants = p.get("variants") or []
            if not _product_is_sellable_per_csv(
                variants, status_by_sku, stock_by_sku=stock_by_sku
            ):
                continue
            row = _row_from_product(p, status_by_sku=status_by_sku)
            reason = _online_store_exclusion_reason(p, stock_by_sku=stock_by_sku)
            if reason:
                skipped.append({**row, "exclusion_reason": reason})
                continue
            candidates.append({**row, "publish_reason": "webshop_uit"})

        if scanned % 5000 == 0 and scanned:
            print(
                f"  ... {scanned} ACTIVE gescand, kandidaten {len(candidates)}, "
                f"overgeslagen {len(skipped)}",
                flush=True,
            )
        url = _next_url_from_link_header(r.headers.get("Link"))
        time.sleep(0.2)

    print(
        f"ACTIVE gescand (Webshop uit): {scanned}; publish-kandidaten {len(candidates)}, "
        f"uitgesloten (zelfde scan) {len(skipped)}",
        flush=True,
    )
    return candidates, skipped


def _gql_node_to_rest_product(node: dict) -> dict:
    """Map GraphQL product-node naar REST-achtige dict voor bestaande filters."""
    images = []
    feat = node.get("featuredImage") or {}
    feat_url = (feat.get("url") or "").strip()
    if feat_url:
        images.append({"src": feat_url})
    variants = []
    for v in ((node.get("variants") or {}).get("nodes")) or []:
        variants.append(
            {
                "sku": v.get("sku") or "",
                "inventory_quantity": v.get("inventoryQuantity"),
            }
        )
    return {
        "id": str(node.get("legacyResourceId") or ""),
        "handle": (node.get("handle") or "").strip(),
        "title": (node.get("title") or "").strip(),
        "product_type": (node.get("productType") or "").strip(),
        "published_at": node.get("publishedAt") or "",
        "images": images,
        "variants": variants,
    }


def _collect_missing_channel_candidates(
    sess: requests.Session,
    status_by_sku: dict[str, str],
    stock_by_sku: dict[str, int],
    *,
    already: set[str],
) -> tuple[list[dict], list[dict]]:
    """
    ACTIVE die al op Webshop staan maar nog ontbrekende kanalen hebben
    (Shop, Facebook, …). Vereist read_publications.
    """
    if not _list_publication_ids(sess):
        return [], []

    scanned = 0
    candidates: list[dict] = []
    skipped: list[dict] = []
    cursor: str | None = None
    pages = 0

    while True:
        body = _graphql_post(
            sess,
            _GQL_PRODUCTS_MISSING_CHANNELS,
            {"cursor": cursor},
        )
        if body.get("errors"):
            err_s = str(body.get("errors"))
            if "ACCESS" in err_s.upper() or "publication" in err_s.lower():
                print(
                    f"  skip ontbrekende-kanalen-scan (geen publications-scope): {err_s[:200]}",
                    flush=True,
                )
                return [], []
            print(f"  warn missing-channels scan: {err_s[:300]}", flush=True)
            break

        conn = ((body.get("data") or {}).get("products")) or {}
        nodes = conn.get("nodes") or []
        page = conn.get("pageInfo") or {}
        pages += 1

        for node in nodes:
            scanned += 1
            unpublished = ((node.get("unpublishedPublications") or {}).get("nodes")) or []
            if not unpublished:
                continue
            if not (node.get("publishedAt") or "").strip():
                # Webshop-uit wordt al door REST-scan opgepakt
                continue
            pid = str(node.get("legacyResourceId") or "")
            if not pid or pid in already:
                continue
            p = _gql_node_to_rest_product(node)
            variants = p.get("variants") or []
            if not _product_is_sellable_per_csv(
                variants, status_by_sku, stock_by_sku=stock_by_sku
            ):
                continue
            row = _row_from_product(p, status_by_sku=status_by_sku)
            reason = _online_store_exclusion_reason(p, stock_by_sku=stock_by_sku)
            if reason:
                skipped.append({**row, "exclusion_reason": reason})
                continue
            missing_names = ", ".join(
                (n.get("name") or n.get("id") or "?") for n in unpublished[:8]
            )
            candidates.append(
                {
                    **row,
                    "publish_reason": f"kanalen_missen:{missing_names}",
                }
            )

        if pages % 40 == 0 and pages:
            print(
                f"  ... missing-channels pages={pages} scanned={scanned} "
                f"kandidaten={len(candidates)}",
                flush=True,
            )
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if not cursor:
            break
        time.sleep(0.15)

    print(
        f"Ontbrekende kanalen: {scanned} ACTIVE bekeken; "
        f"+{len(candidates)} kandidaten, {len(skipped)} uitgesloten",
        flush=True,
    )
    return candidates, skipped


def _collect_all_publish_candidates(
    sess: requests.Session,
    status_by_sku: dict[str, str],
    stock_by_sku: dict[str, int],
) -> tuple[list[dict], list[dict]]:
    candidates, skipped = _collect_candidates_rest(sess, status_by_sku, stock_by_sku)
    have = {r["product_id"] for r in candidates if r.get("product_id")}
    extra, skipped2 = _collect_missing_channel_candidates(
        sess, status_by_sku, stock_by_sku, already=have
    )
    candidates.extend(extra)
    skipped.extend(skipped2)
    return candidates, skipped


def _collect_unpublish_rest(
    sess: requests.Session,
    status_by_sku: dict[str, str],
    stock_by_sku: dict[str, int],
) -> list[dict]:
    """ACTIVE + op webshop + moet UIT (type/afbeelding/uitverkocht/ERP stock 0)."""
    headers = {"X-Shopify-Access-Token": TOKEN}
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        f"?limit=250&status=active"
        f"&fields=id,handle,title,status,published_at,product_type,images,variants"
    )
    scanned = 0
    to_unpublish: list[dict] = []

    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(2)
            continue
        if r.status_code >= 500:
            time.sleep(3)
            continue
        r.raise_for_status()

        for p in r.json().get("products", []):
            scanned += 1
            if not (p.get("published_at") or "").strip():
                continue
            reason = _online_store_exclusion_reason(p, stock_by_sku=stock_by_sku)
            if not reason:
                continue
            to_unpublish.append(
                {**_row_from_product(p, status_by_sku=status_by_sku), "exclusion_reason": reason}
            )

        if scanned % 5000 == 0 and scanned:
            print(
                f"  ... unpublish-scan {scanned} ACTIVE, te depubliceren {len(to_unpublish)}",
                flush=True,
            )
        url = _next_url_from_link_header(r.headers.get("Link"))
        time.sleep(0.2)

    print(
        f"Unpublish-scan: {scanned} ACTIVE; te depubliceren {len(to_unpublish)}",
        flush=True,
    )
    return to_unpublish


def _load_candidates_from_csv(path: Path) -> list[dict]:
    rows: list[dict] = []
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            pid = (row.get("product_id") or "").strip()
            if not pid:
                continue
            rows.append(
                {
                    "product_id": pid,
                    "handle": (row.get("handle") or "").strip(),
                    "title": (row.get("title") or "").strip()[:120],
                    "product_type": (row.get("product_type") or "").strip(),
                    "skus": (row.get("skus") or "").strip(),
                    "article_statuses": (row.get("article_statuses") or "").strip(),
                    "variant_count": (row.get("variant_count") or "").strip(),
                }
            )
    return rows


def _fetch_products_by_ids(
    sess: requests.Session,
    product_ids: list[str],
) -> dict[str, dict]:
    headers = {"X-Shopify-Access-Token": TOKEN}
    out: dict[str, dict] = {}
    ids = [pid for pid in product_ids if str(pid).isdigit()]
    fields = "id,handle,title,status,published_at,product_type,images,variants"
    for i in range(0, len(ids), 250):
        chunk = ids[i : i + 250]
        url = (
            f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
            f"?ids={','.join(chunk)}&fields={fields}"
        )
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        for p in r.json().get("products", []):
            out[str(p.get("id") or "")] = p
        time.sleep(0.15)
    return out


def _apply_online_store_rules(
    sess: requests.Session,
    rows: list[dict],
    *,
    status_by_sku: dict[str, str] | None = None,
    stock_by_sku: dict[str, int] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Filter kandidaten op type/afbeelding (haalt actuele Shopify-productdata op)."""
    if not rows:
        return [], []
    products = _fetch_products_by_ids(sess, [r["product_id"] for r in rows])
    candidates: list[dict] = []
    skipped: list[dict] = []
    for row in rows:
        p = products.get(row["product_id"])
        if not p:
            skipped.append({**row, "exclusion_reason": "niet_gevonden"})
            continue
        merged = _row_from_product(p, status_by_sku=status_by_sku)
        if row.get("article_statuses"):
            merged["article_statuses"] = row["article_statuses"]
        reason = _online_store_exclusion_reason(p, stock_by_sku=stock_by_sku)
        if reason:
            skipped.append({**merged, "exclusion_reason": reason})
        else:
            candidates.append(merged)
    return candidates, skipped


def _list_publication_ids(sess: requests.Session) -> list[str]:
    """Alle sales-channel publication IDs. Leeg als read_publications ontbreekt."""
    global _PUBLICATION_IDS
    if _PUBLICATION_IDS is not None:
        return _PUBLICATION_IDS
    body = _graphql_post(sess, _GQL_PUBLICATIONS)
    if body.get("errors"):
        print(f"  warn publications: {body.get('errors')}", flush=True)
        _PUBLICATION_IDS = []
        return _PUBLICATION_IDS
    nodes = (((body.get("data") or {}).get("publications") or {}).get("nodes")) or []
    ids: list[str] = []
    for n in nodes:
        pid = (n.get("id") or "").strip()
        if not pid:
            continue
        ids.append(pid)
        name = (n.get("name") or "").strip()
        if name:
            _PUBLICATION_NAMES[pid] = name
    _PUBLICATION_IDS = ids
    if ids:
        names = ", ".join(_PUBLICATION_NAMES.get(i, i) for i in ids)
        print(f"Publicatiekanalen: {len(ids)} ({names})", flush=True)
    else:
        print(
            "Publicatiekanalen: 0 — app mist read_publications/write_publications; "
            "fallback alleen Online Store (Webshop).",
            flush=True,
        )
    return _PUBLICATION_IDS


def _set_online_store_published(
    sess: requests.Session,
    product_id: str,
    *,
    published: bool,
) -> tuple[bool, str, str | None]:
    gid = f"gid://shopify/Product/{product_id}"
    body = _graphql_post(
        sess,
        _GQL_PRODUCT_PUBLISHED,
        {
            "input": {
                "id": gid,
                "status": "ACTIVE",
                "published": published,
            }
        },
    )
    gerrs = body.get("errors")
    if gerrs:
        return False, json.dumps(gerrs)[:500], None
    upd = ((body.get("data") or {}).get("productUpdate")) or {}
    user_errors = upd.get("userErrors") or []
    if user_errors:
        return False, json.dumps(user_errors)[:500], None
    product = upd.get("product") or {}
    pub = product.get("publishedAt")
    if published and not pub:
        return False, "publishedAt nog steeds leeg na productUpdate", None
    if not published and pub:
        return False, "publishedAt nog gevuld na depublicatie", None
    return True, "", str(pub) if pub else None


def _publishable_set_channels(
    sess: requests.Session,
    product_id: str,
    publication_ids: list[str],
    *,
    publish: bool,
) -> str:
    if not publication_ids:
        return ""
    mutation = _GQL_PUBLISHABLE_PUBLISH if publish else _GQL_PUBLISHABLE_UNPUBLISH
    field = "publishablePublish" if publish else "publishableUnpublish"
    body = _graphql_post(
        sess,
        mutation,
        {
            "id": f"gid://shopify/Product/{product_id}",
            "input": [{"publicationId": pid} for pid in publication_ids],
        },
    )
    if body.get("errors"):
        err_s = str(body.get("errors"))
        if "ACCESS" in err_s.upper() or "publication" in err_s.lower():
            return f"scope:{err_s[:200]}"
        return err_s[:300]
    uerr = (((body.get("data") or {}).get(field)) or {}).get("userErrors") or []
    return str(uerr)[:400] if uerr else ""


def _publish_product(sess: requests.Session, product_id: str) -> tuple[bool, str, str | None]:
    """ACTIVE + alle verkoopkanalen (fallback: alleen Online Store)."""
    ok, err, pub_at = _set_online_store_published(sess, product_id, published=True)
    if not ok:
        return False, err, None
    pub_ids = _list_publication_ids(sess)
    if not pub_ids:
        return True, "", pub_at
    ch_err = _publishable_set_channels(sess, product_id, pub_ids, publish=True)
    if ch_err:
        # Shop/Facebook weigeren soms (eligibility); Webshop staat al aan → waarschuwing.
        print(f"  warn kanalen product {product_id}: {ch_err}", flush=True)
    return True, "", pub_at


def _unpublish_product(sess: requests.Session, product_id: str) -> tuple[bool, str]:
    """Van alle kanalen af + Online Store uit."""
    pub_ids = _list_publication_ids(sess)
    if pub_ids:
        ch_err = _publishable_set_channels(sess, product_id, pub_ids, publish=False)
        if ch_err and not ch_err.startswith("scope:"):
            print(f"  warn unpublish kanalen {product_id}: {ch_err}", flush=True)
    ok, err, _ = _set_online_store_published(sess, product_id, published=False)
    return ok, err


def _supabase_headers() -> dict[str, str] | None:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        return None
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _flush_mirror_published(
    sess: requests.Session,
    published_rows: list[tuple[str, str]],
) -> None:
    headers = _supabase_headers()
    base = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    if not headers or not base or not published_rows:
        return
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    chunk = 200
    for i in range(0, len(published_rows), chunk):
        part = published_rows[i : i + chunk]
        rows = [
            {
                "shopify_product_id": int(pid),
                "status": "active",
                "published_at": pub_at,
                "synced_at": ts,
            }
            for pid, pub_at in part
            if str(pid).isdigit() and pub_at
        ]
        if not rows:
            continue
        url = f"{base}/rest/v1/shopify_products"
        params = {"on_conflict": "shopify_product_id"}
        r = sess.post(
            url,
            headers={**headers, "Prefer": "resolution=merge-duplicates,return=minimal"},
            params=params,
            json=rows,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code >= 400:
            print(
                f"Mirror-update waarschuwing (HTTP {r.status_code}): {r.text[:300]}",
                flush=True,
            )


def _flush_mirror_unpublished(
    sess: requests.Session,
    product_ids: list[str],
) -> None:
    headers = _supabase_headers()
    base = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    if not headers or not base or not product_ids:
        return
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    chunk = 200
    for i in range(0, len(product_ids), chunk):
        part = [pid for pid in product_ids[i : i + chunk] if str(pid).isdigit()]
        if not part:
            continue
        rows = [
            {
                "shopify_product_id": int(pid),
                "status": "active",
                "published_at": None,
                "synced_at": ts,
            }
            for pid in part
        ]
        url = f"{base}/rest/v1/shopify_products"
        params = {"on_conflict": "shopify_product_id"}
        r = sess.post(
            url,
            headers={**headers, "Prefer": "resolution=merge-duplicates,return=minimal"},
            params=params,
            json=rows,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code >= 400:
            print(
                f"Mirror-unpublish waarschuwing (HTTP {r.status_code}): {r.text[:300]}",
                flush=True,
            )


def _write_report(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=";", extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Voer publicatie echt uit (zonder vlag: alleen scan + rapport)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximaal aantal producten om te publiceren (0 = geen limiet)",
    )
    ap.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        metavar="PAD",
        help="Gebruik vaste product_id-lijst i.p.v. volledige catalogusscan",
    )
    ap.add_argument(
        "--reuse-candidates",
        action="store_true",
        help=(
            "Bij --apply: lees kandidaten uit --output-csv (geen REST-catalogusscan). "
            "Handig na een eerdere dry-run in dezelfde sessie."
        ),
    )
    ap.add_argument(
        "--output-csv",
        type=Path,
        default=Path("output/publish_sellable_active_candidates.csv"),
        help="Rapport met kandidaten (default: output/publish_sellable_active_candidates.csv)",
    )
    ap.add_argument(
        "--published-csv",
        type=Path,
        default=Path("output/publish_sellable_active_published.csv"),
        help="Rapport na --apply met geslaagde publicaties",
    )
    ap.add_argument(
        "--errors-csv",
        type=Path,
        default=Path("output/publish_sellable_active_errors.csv"),
        help="Rapport na --apply met mislukte publicaties",
    )
    ap.add_argument(
        "--skipped-csv",
        type=Path,
        default=Path("output/publish_sellable_active_skipped.csv"),
        help="Verkoopbare ACTIVE zonder webshop, maar uitgesloten (type/afbeelding)",
    )
    ap.add_argument(
        "--unpublish-csv",
        type=Path,
        default=Path("output/publish_sellable_active_unpublish.csv"),
        help="ACTIVE op webshop die UIT moeten (type/afbeelding)",
    )
    ap.add_argument(
        "--unpublished-csv",
        type=Path,
        default=Path("output/publish_sellable_active_unpublished.csv"),
        help="Rapport na --apply met geslaagde depublicaties",
    )
    ap.add_argument(
        "--unpublish-errors-csv",
        type=Path,
        default=Path("output/publish_sellable_active_unpublish_errors.csv"),
        help="Rapport na --apply met mislukte depublicaties",
    )
    ap.add_argument(
        "--skip-unpublish",
        action="store_true",
        help="Geen depublicatie-scan/-mutaties (alleen publiceren)",
    )
    ap.add_argument(
        "--require-status-index",
        action="store_true",
        help="Faal als er geen ArticleStatus-index uit CSV geladen kan worden",
    )
    args = ap.parse_args()

    if not TOKEN or not SHOP:
        print(
            "SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN zijn verplicht (.env).",
            file=sys.stderr,
        )
        return 2

    status_by_sku = load_article_status_from_35_z1_csv_files()
    stock_by_sku = load_stock_available_from_35_z1_csv_files()
    if status_by_sku:
        print(
            f"CSV ArticleStatus-index (KTM+HSQ+WP): {len(status_by_sku)} SKU's.",
            flush=True,
        )
    elif args.require_status_index:
        print(
            "FOUT: geen ArticleStatus-index uit CSV — stop.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    else:
        print(
            "Waarschuwing: geen ArticleStatus-index; er worden geen kandidaten gevonden.",
            flush=True,
        )
    if stock_by_sku:
        print(
            f"CSV StockAvailable-index (KTM+HSQ+WP): {len(stock_by_sku)} SKU's.",
            flush=True,
        )
    else:
        print(
            "Waarschuwing: geen StockAvailable-index; alleen Shopify-voorraad telt voor uitverkocht.",
            flush=True,
        )

    sess = _http_session()

    csv_source: Path | None = None
    if args.input_csv and args.input_csv.is_file():
        csv_source = args.input_csv
    elif args.reuse_candidates:
        if not args.output_csv.is_file():
            print(
                f"FOUT: --reuse-candidates maar {args.output_csv} bestaat niet. "
                "Draai eerst een dry-run.",
                file=sys.stderr,
                flush=True,
            )
            return 2
        csv_source = args.output_csv

    report_fields = [
        "product_id",
        "handle",
        "title",
        "product_type",
        "skus",
        "article_statuses",
        "variant_count",
        "publish_reason",
    ]
    skipped_fields = [
        "product_id",
        "handle",
        "title",
        "product_type",
        "skus",
        "article_statuses",
        "variant_count",
        "exclusion_reason",
    ]

    # Warm publications cache early (duidelijke logregel vóór lange scan).
    _list_publication_ids(sess)

    skipped: list[dict] = []
    if csv_source is not None:
        print(f"Kandidaten uit CSV (geen catalogusscan): {csv_source}", flush=True)
        raw_rows = _load_candidates_from_csv(csv_source)
        candidates, skipped = _apply_online_store_rules(
            sess,
            raw_rows,
            status_by_sku=status_by_sku,
            stock_by_sku=stock_by_sku,
        )
        for row in candidates:
            row.setdefault("publish_reason", "csv")
    else:
        print(
            "Catalogusscan: ACTIVE verkoopbaar + Webshop uit óf ontbrekende "
            "kanalen (Shop/Facebook/…)…",
            flush=True,
        )
        candidates, skipped = _collect_all_publish_candidates(
            sess, status_by_sku, stock_by_sku
        )

    to_unpublish: list[dict] = []
    if not args.skip_unpublish:
        print(
            "Unpublish-scan: ACTIVE op webshop met type/afbeelding/uitverkocht/ERP stock 0...",
            flush=True,
        )
        to_unpublish = _collect_unpublish_rest(sess, status_by_sku, stock_by_sku)

    candidates.sort(key=lambda r: (r.get("handle") or "").lower())
    skipped.sort(key=lambda r: (r.get("handle") or "").lower())
    to_unpublish.sort(key=lambda r: (r.get("handle") or "").lower())

    _write_report(args.output_csv, candidates, report_fields)
    print(f"Publish-kandidaten: {len(candidates)} → {args.output_csv}", flush=True)
    if skipped:
        _write_report(args.skipped_csv, skipped, skipped_fields)
        print(f"Overgeslagen (publish): {len(skipped)} → {args.skipped_csv}", flush=True)
    if to_unpublish:
        _write_report(args.unpublish_csv, to_unpublish, skipped_fields)
        print(f"Te depubliceren: {len(to_unpublish)} → {args.unpublish_csv}", flush=True)

    if not args.apply:
        print("Dry-run: geen wijzigingen in Shopify.", flush=True)
        for row in candidates[:10]:
            print(
                f"  publish {row['handle']}: {row['title'][:45]} | "
                f"{row.get('product_type', '')} | ERP {row['article_statuses']} | "
                f"{row.get('publish_reason', '')}",
                flush=True,
            )
        if len(candidates) > 10:
            print(f"  ... +{len(candidates) - 10} publish-kandidaten in CSV", flush=True)
        for row in to_unpublish[:5]:
            print(
                f"  unpublish {row['handle']}: {row.get('exclusion_reason')} | "
                f"type {row.get('product_type', '')}",
                flush=True,
            )
        if len(to_unpublish) > 5:
            print(f"  ... +{len(to_unpublish) - 5} te depubliceren in CSV", flush=True)
        return 0

    sleep_sec = float(
        (os.environ.get("SHOPIFY_PUBLISH_SLEEP_SEC") or "").strip() or "0.25"
    )
    progress_every = max(50, int(os.environ.get("SHOPIFY_PUBLISH_PROGRESS_EVERY", "100")))

    unpublish_err_count = 0
    if to_unpublish and not args.skip_unpublish:
        unpublish_ok: list[dict] = []
        unpublish_err: list[dict] = []
        mirror_unpub: list[str] = []
        print(f"Depubliceren alle kanalen: {len(to_unpublish)} producten...", flush=True)
        for idx, row in enumerate(to_unpublish, start=1):
            pid = row["product_id"]
            ok, err = _unpublish_product(sess, pid)
            if ok:
                unpublish_ok.append(row)
                mirror_unpub.append(pid)
            else:
                unpublish_err.append({**row, "error": err})
            if idx == 1 or idx == len(to_unpublish) or idx % progress_every == 0:
                print(
                    f"  unpublish {idx}/{len(to_unpublish)} "
                    f"ok={len(unpublish_ok)} fout={len(unpublish_err)}",
                    flush=True,
                )
            if len(mirror_unpub) >= 200:
                _flush_mirror_unpublished(sess, mirror_unpub)
                mirror_unpub.clear()
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        _flush_mirror_unpublished(sess, mirror_unpub)
        unpublish_err_count = len(unpublish_err)
        _write_report(args.unpublished_csv, unpublish_ok, skipped_fields)
        _write_report(args.unpublish_errors_csv, unpublish_err, [*skipped_fields, "error"])
        print(
            f"Depublicatie klaar: {len(unpublish_ok)} ok, {unpublish_err_count} fouten.",
            flush=True,
        )

    to_publish = candidates
    if args.limit > 0:
        to_publish = candidates[: args.limit]
        print(f"--limit {args.limit}: alleen eerste {len(to_publish)} producten.", flush=True)

    published_rows: list[dict] = []
    error_rows: list[dict] = []
    mirror_flush: list[tuple[str, str]] = []

    print(f"Publiceren op alle kanalen: {len(to_publish)} producten...", flush=True)
    for idx, row in enumerate(to_publish, start=1):
        pid = row["product_id"]
        ok, err, pub_at = _publish_product(sess, pid)
        if ok:
            published_rows.append({**row, "published_at": pub_at or ""})
            if pub_at:
                mirror_flush.append((pid, pub_at))
        else:
            error_rows.append({**row, "error": err})

        if idx == 1 or idx == len(to_publish) or idx % progress_every == 0:
            print(
                f"  {idx}/{len(to_publish)} ok={len(published_rows)} fout={len(error_rows)}",
                flush=True,
            )
        if len(mirror_flush) >= 200:
            _flush_mirror_published(sess, mirror_flush)
            mirror_flush.clear()
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    _flush_mirror_published(sess, mirror_flush)

    _write_report(
        args.published_csv,
        published_rows,
        [*report_fields, "published_at"],
    )
    _write_report(
        args.errors_csv,
        error_rows,
        [*report_fields, "error"],
    )

    print(
        f"Klaar: gepubliceerd {len(published_rows)}, publish-fouten {len(error_rows)}.",
        flush=True,
    )
    print(f"  Succes: {args.published_csv}", flush=True)
    if error_rows:
        print(f"  Publish-fouten: {args.errors_csv}", flush=True)
    return 1 if error_rows or unpublish_err_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
