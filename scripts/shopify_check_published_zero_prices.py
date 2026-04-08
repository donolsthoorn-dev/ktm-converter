#!/usr/bin/env python3
"""
Lijst gepubliceerde Shopify-producten waar een variant geen prijs heeft of prijs 0.

Standaard: **Bulk Operations** (GraphQL) — één achtergrondexport + JSONL-download; geschikt
voor zeer grote catalogi (bv. 80k+ producten). Veel sneller dan REST-paginering.

Alleen producten met status ACTIVE en een gezet publishedAt (zichtbaar in de winkel).

Voorbeeld (vanaf projectroot):

  python3 scripts/shopify_check_published_zero_prices.py

Langzame fallback (REST, pagina voor pagina):

  python3 scripts/shopify_check_published_zero_prices.py --rest

Vereist: SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN in .env (zie .env.example).
Bulk gebruikt dezelfde token; offline/long-lived token aanbevolen voor lange jobs.

Exitcode: 0 als alles in orde is, 1 als er minstens één probleemvariant is, 2 bij configuratiefout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
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

import config  # noqa: E402 — laadt .env via config

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)
_REQUEST_TIMEOUT_LONG = (12, 600)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        publishedAt
        variants {
          edges {
            node {
              id
              sku
              title
              price
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation KtmBulkPriceCheck {
  bulkOperationRunQuery(
    query: BULK_QUERY_PLACEHOLDER
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

_GQL_POLL = """
query KtmPollBulk($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      status
      errorCode
      objectCount
      url
      partialDataUrl
    }
  }
}
"""


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
            timeout=_REQUEST_TIMEOUT_LONG,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return last
    return last


def _variant_price_bad(raw: object) -> bool:
    if raw is None:
        return True
    if isinstance(raw, (int, float)):
        try:
            return float(raw) <= 0.0
        except (TypeError, ValueError):
            return True
    s = str(raw).strip()
    if not s:
        return True
    try:
        return float(s.replace(",", ".")) <= 0.0
    except ValueError:
        return True


def _product_published_active_gql(p: dict) -> bool:
    st = (p.get("status") or "").strip().upper()
    if st != "ACTIVE":
        return False
    pub = p.get("publishedAt")
    if pub is None:
        return False
    if isinstance(pub, str) and not pub.strip():
        return False
    return True


def _gid_numeric(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]


def _run_bulk(sess: requests.Session) -> int:
    q = _GQL_BULK_START.replace(
        "BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY)
    )
    body = _graphql_post(sess, q)
    gerrs = body.get("errors")
    if gerrs:
        print("GraphQL-fout bij start bulk export:", file=sys.stderr)
        print(json.dumps(gerrs, indent=2), file=sys.stderr)
        return 2

    data = body.get("data") or {}
    run = data.get("bulkOperationRunQuery") or {}
    uerr = run.get("userErrors") or []
    if uerr:
        print("Bulk export geweigerd:", file=sys.stderr)
        for e in uerr:
            print(f"  {e.get('field')}: {e.get('message')}", file=sys.stderr)
        return 2

    bulk = run.get("bulkOperation") or {}
    op_id = bulk.get("id")
    if not op_id:
        print("Geen bulk operation id in response.", file=sys.stderr)
        return 2

    print(
        "Bulk export gestart — Shopify bouwt één JSONL-bestand (kan enkele minuten duren)...",
        flush=True,
    )

    poll_interval = 3.0
    while True:
        poll = _graphql_post(sess, _GQL_POLL, {"id": op_id})
        if poll.get("errors"):
            print("Poll-fout:", poll.get("errors"), file=sys.stderr)
            time.sleep(poll_interval)
            continue

        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        count = node.get("objectCount")
        if count is not None:
            print(f"  Status: {status} — objecten verwerkt: {count}", flush=True)
        else:
            print(f"  Status: {status}", flush=True)

        if status == "COMPLETED":
            url = node.get("url")
            if not url:
                print("\nGeen resultaat-URL (lege shop?).")
                return 0
            return _download_and_scan_jsonl(sess, url)

        if status in ("FAILED", "CANCELED"):
            err = node.get("errorCode")
            print(f"Bulk export {status.lower()}: {err}", file=sys.stderr)
            purl = node.get("partialDataUrl")
            if purl:
                print("Er is gedeeltelijke data; parse die niet automatisch.", file=sys.stderr)
            return 2

        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)


def _download_and_scan_jsonl(sess: requests.Session, url: str) -> int:
    print("JSONL downloaden en regel voor regel verwerken...", flush=True)
    products: dict[str, dict] = {}
    raw: list[tuple[dict, dict]] = []
    prev_product_gid: str | None = None

    r = sess.get(
        url,
        stream=True,
        timeout=_REQUEST_TIMEOUT_LONG,
        proxies={"http": None, "https": None},
    )
    r.raise_for_status()

    line_n = 0
    for raw_line in r.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        line_n += 1
        if line_n % 250_000 == 0:
            print(f"  ... {line_n} JSONL-regels", flush=True)
        try:
            obj = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        gid = (obj.get("id") or "").strip()
        if "/ProductVariant/" in gid:
            parent = obj.get("__parentId")
            if not parent or parent not in products:
                continue
            p = products[parent]
            if _variant_price_bad(obj.get("price")):
                raw.append((p, obj))
            continue
        if "/Product/" in gid and "/ProductVariant/" not in gid:
            # JSONL-volgorde: product, daarna varianten; geheugen vrijmaken vóór volgend product
            if prev_product_gid and prev_product_gid in products:
                del products[prev_product_gid]
            prev_product_gid = gid
            if _product_published_active_gql(obj):
                products[gid] = obj
            continue

    grouped: dict[str, tuple[dict, list[dict]]] = {}
    for p, v in raw:
        pid = p.get("id", "")
        if pid not in grouped:
            grouped[pid] = (p, [])
        grouped[pid][1].append(v)

    return _print_report(grouped)


def _print_report(grouped: dict[str, tuple[dict, list[dict]]]) -> int:
    admin_base = f"https://{SHOP}/admin/products"
    if not grouped:
        print("\nGeen gepubliceerde producten met ontbrekende of nul-prijs (variants).")
        return 0

    print(
        f"\nGevonden: {len(grouped)} product(en) met minstens één variant zonder geldige prijs (> 0):\n",
        flush=True,
    )
    for _pid, (p, variants) in sorted(
        grouped.items(),
        key=lambda kv: (kv[1][0].get("title") or "").lower(),
    ):
        pgid = p.get("id") or ""
        title = (p.get("title") or "").strip() or "(zonder titel)"
        handle = (p.get("handle") or "").strip()
        print(f"— {title}")
        if handle:
            print(f"  Handle: {handle}")
        if pgid:
            print(f"  Admin: {admin_base}/{_gid_numeric(pgid)}")
        for v in variants:
            vid = v.get("id", "")
            vtitle = (v.get("title") or "").strip() or "(variant)"
            sku = (v.get("sku") or "").strip() or "(geen SKU)"
            pr = v.get("price")
            pr_disp = repr(pr) if pr is not None else "(null)"
            print(f"  Variant: {vtitle} | SKU: {sku} | prijs: {pr_disp} (id: {vid})")
        print(flush=True)

    return 1


def _next_url_from_link_header(link: str | None) -> str | None:
    if not link:
        return None
    for part in link.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().replace("<", "").replace(">", "")
    return None


def _run_rest(sess: requests.Session) -> int:
    headers = {"X-Shopify-Access-Token": TOKEN}
    fields = "id,handle,title,status,published_at,variants"
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        f"?limit=250&fields={fields}"
    )

    grouped: dict[str, tuple[dict, list[dict]]] = {}
    scanned = 0

    print("Shopify-producten scannen via REST (langzaam bij grote catalogi)...", flush=True)

    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("Rate limit, wachten...", flush=True)
            time.sleep(2)
            continue
        if r.status_code >= 500:
            print("Shopify serverfout, retry...", flush=True)
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()

        for p in data.get("products", []):
            scanned += 1
            if (p.get("status") or "").lower() != "active":
                continue
            if not (p.get("published_at") or "").strip():
                continue

            bad: list[dict] = []
            for v in p.get("variants") or []:
                if _variant_price_bad(v.get("price")):
                    bad.append(v)
            if bad:
                pid = str(p.get("id", ""))
                grouped[pid] = (p, bad)

        print(f"  ... {scanned} producten verwerkt", flush=True)
        url = _next_url_from_link_header(r.headers.get("Link"))
        time.sleep(0.25)

    return _print_report(grouped)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Gepubliceerde producten met ontbrekende of nul-prijs (variant)"
    )
    ap.add_argument(
        "--rest",
        action="store_true",
        help="REST-paginering i.p.v. bulk export (langzaam; alleen voor debug)",
    )
    args = ap.parse_args()

    if not TOKEN:
        print(
            "Geen SHOPIFY_ACCESS_TOKEN — zet deze in .env (zie .env.example).",
            file=sys.stderr,
        )
        return 2

    sess = _http_session()
    if args.rest:
        return _run_rest(sess)
    return _run_bulk(sess)


if __name__ == "__main__":
    raise SystemExit(main())
