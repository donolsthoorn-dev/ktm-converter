#!/usr/bin/env python3
"""
Lijst alle Shopify-producten met status **DRAFT** als CSV (inclusief producttype).

GraphQL met zoekfilter `status:draft` — alleen concept-producten, geen volledige catalogus.

Extra kolom **article_status**: KTM **ArticleStatus** uit alle `*35_Z1_EUR_EN_csv.csv` onder
`input/` (match op variant-SKU ↔ ArticleNumber), zie `modules.pricing_loader`.

  python3 scripts/shopify_list_draft_products.py > output/draft_products.csv
  python3 scripts/shopify_list_draft_products.py -o output/draft_products.csv
  python3 scripts/shopify_list_draft_products.py --input-dir pad/naar/input

Voortgang naar stderr. Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN in .env.
"""

from __future__ import annotations

import argparse
import csv
import glob
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

import config  # noqa: E402
from modules.pricing_loader import (  # noqa: E402
    load_article_status_from_35_z1_csv_files,
    normalize_sku_key,
)

SHOP = config.SHOPIFY_SHOP_DOMAIN
INPUT_DIR = config.INPUT_DIR
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)

_GQL_PAGE = """
query DraftProducts($cursor: String) {
  products(first: 250, after: $cursor, query: "status:draft") {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        handle
        title
        productType
        status
        variants(first: 250) {
          edges {
            node {
              sku
            }
          }
        }
      }
    }
  }
}
"""


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gid_numeric(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]


def _graphql_post(sess: requests.Session, query: str, variables: dict | None) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
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
        r.raise_for_status()
        body = r.json()
        errs = body.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return body
    return body


def _variant_skus_from_node(node: dict) -> list[str]:
    out: list[str] = []
    variants = ((node.get("variants") or {}).get("edges")) or []
    for e in variants:
        n = (e or {}).get("node") or {}
        sku = (n.get("sku") or "").strip()
        if sku:
            out.append(sku)
    return out


def _article_status_for_skus(
    skus: list[str],
    status_by_article: dict[str, str],
) -> str:
    """Unieke ArticleStatus-waarden voor gegeven SKU's, gescheiden door '; '."""
    seen_order: list[str] = []
    seen_set: set[str] = set()
    for raw in skus:
        k = normalize_sku_key(raw)
        if not k:
            continue
        st = status_by_article.get(k)
        if st is None:
            continue
        if st in seen_set:
            continue
        seen_set.add(st)
        seen_order.append(st)
    return "; ".join(seen_order)


def fetch_all_draft_rows(
    sess: requests.Session,
    status_by_article: dict[str, str],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    cursor: str | None = None
    page = 0
    while True:
        page += 1
        body = _graphql_post(sess, _GQL_PAGE, {"cursor": cursor})
        if body.get("errors"):
            print(
                "GraphQL-fout:",
                json.dumps(body["errors"], indent=2),
                file=sys.stderr,
            )
            raise SystemExit(2)

        data = body.get("data") or {}
        conn = (data.get("products")) or {}
        edges = conn.get("edges") or []
        for e in edges:
            node = (e or {}).get("node") or {}
            gid = node.get("id") or ""
            if not gid:
                continue
            skus = _variant_skus_from_node(node)
            rows.append(
                {
                    "product_id_numeric": _gid_numeric(str(gid)),
                    "handle": (node.get("handle") or "").strip(),
                    "title": (node.get("title") or "").strip(),
                    "product_type": (node.get("productType") or "").strip(),
                    "status": (node.get("status") or "").strip().upper(),
                    "article_status": _article_status_for_skus(skus, status_by_article),
                }
            )

        print(
            f"Pagina {page}: +{len(edges)} — totaal {len(rows)} draft-producten",
            file=sys.stderr,
            flush=True,
        )

        pi = conn.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        if not cursor:
            break
        time.sleep(0.25)

    return rows


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Exporteer alle DRAFT Shopify-producten naar CSV (met producttype)."
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="PAD",
        help="Schrijf naar bestand i.p.v. stdout",
    )
    ap.add_argument(
        "--input-dir",
        type=Path,
        metavar="MAP",
        default=None,
        help=f"Map met *35_Z1_EUR_EN_csv.csv (default: {INPUT_DIR})",
    )
    args = ap.parse_args()

    if not TOKEN or not SHOP:
        print(
            "SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN zijn verplicht (.env).",
            file=sys.stderr,
        )
        return 2

    in_base = os.path.normpath(str(args.input_dir or INPUT_DIR))
    pattern = os.path.join(in_base, "*35_Z1_EUR_EN_csv.csv")
    matched = sorted(glob.glob(pattern))
    status_by_article = load_article_status_from_35_z1_csv_files(in_base)
    if matched:
        print(
            f"KTM ArticleStatus: {len(matched)} bestand(en) ({in_base}/*35_Z1_EUR_EN_csv.csv), "
            f"{len(status_by_article)} SKU's in index.",
            file=sys.stderr,
            flush=True,
        )
    else:
        print(
            f"Geen *35_Z1_EUR_EN_csv.csv gevonden onder {in_base!r} — kolom article_status blijft leeg.",
            file=sys.stderr,
            flush=True,
        )

    sess = _http_session()
    rows = fetch_all_draft_rows(sess, status_by_article)

    fieldnames = [
        "product_id_numeric",
        "handle",
        "title",
        "product_type",
        "status",
        "article_status",
    ]

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        out_f = open(args.output, "w", encoding="utf-8", newline="")
        close_out = True
    else:
        out_f = sys.stdout
        close_out = False

    try:
        w = csv.DictWriter(out_f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    finally:
        if close_out:
            out_f.close()

    print(f"Klaar: {len(rows)} rijen.", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
