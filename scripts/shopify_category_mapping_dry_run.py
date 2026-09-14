#!/usr/bin/env python3
"""
Dry-run: welke Shopify Category zou elk product zonder category krijgen?

Gebruikt modules.category_mapper.resolve_shopify_product_category
(Type → tags → titel/body → default).

  python3 scripts/shopify_category_mapping_dry_run.py
  python3 scripts/shopify_category_mapping_dry_run.py --shop motox
  python3 scripts/shopify_category_mapping_dry_run.py --limit 500

Schrijft CSV onder output/. Geen writes naar Shopify (alleen lezen).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.category_mapper import (  # noqa: E402
    is_missing_shopify_category,
    resolve_shopify_product_category,
)

_REQUEST_TIMEOUT = (15, 120)


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        raise SystemExit(f"Missing {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, shop: str, token: str, api: str, query: str, variables: dict | None = None) -> dict:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    for attempt in range(8):
        r = sess.post(
            url,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            json={"query": query, "variables": variables or {}},
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(1.5 + attempt)
            continue
        r.raise_for_status()
        body = r.json()
        if body.get("errors"):
            if "Throttl" in str(body["errors"]):
                time.sleep(2 + attempt)
                continue
            raise RuntimeError(body["errors"])
        return body
    raise RuntimeError("GraphQL failed")


QUERY = """
query ($c: String) {
  products(first: 50, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      status
      productType
      tags
      descriptionHtml
      category { fullName }
    }
  }
}
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument("--limit", type=int, default=0, help="Max producten te scannen (0=alle)")
    ap.add_argument(
        "--only-empty",
        action="store_true",
        default=True,
        help="Alleen producten zonder category (default)",
    )
    ap.add_argument("--include-filled", action="store_true", help="Ook producten mét category")
    args = ap.parse_args()
    only_empty = not args.include_filled

    env_path = ROOT / (".env.motox" if args.shop == "motox" else ".env")
    env = _load_dotenv(env_path)
    shop = env["SHOPIFY_SHOP_DOMAIN"]
    token = env["SHOPIFY_ACCESS_TOKEN"]
    api = env.get("SHOPIFY_ADMIN_API_VERSION", "2024-10")

    sess = _session()
    print(f"Shop: {shop}  only_empty={only_empty}", flush=True)

    rows: list[dict] = []
    source_counts: Counter[str] = Counter()
    bucket_counts: Counter[str] = Counter()
    cursor = None
    scanned = 0
    empty = 0

    while True:
        body = _gql(sess, shop, token, api, QUERY, {"c": cursor})
        conn = body["data"]["products"]
        for p in conn["nodes"]:
            scanned += 1
            cat = (p.get("category") or {}).get("fullName") if p.get("category") else None
            if only_empty and not is_missing_shopify_category(cat):
                continue
            if only_empty:
                empty += 1
            decision = resolve_shopify_product_category(
                product_type=p.get("productType"),
                tags=p.get("tags") or [],
                title=p.get("title"),
                body_html=p.get("descriptionHtml"),
            )
            source_counts[decision.source.split(":")[0]] += 1
            bucket_counts[decision.bucket] += 1
            rows.append(
                {
                    "product_id": (p.get("id") or "").split("/")[-1],
                    "handle": p.get("handle"),
                    "title": p.get("title"),
                    "status": p.get("status"),
                    "product_type": p.get("productType") or "",
                    "tags": ", ".join(p.get("tags") or []),
                    "current_category": cat or "",
                    "proposed_category": decision.path,
                    "proposed_bucket": decision.bucket,
                    "source": decision.source,
                }
            )
            if args.limit and scanned >= args.limit:
                break
        if args.limit and scanned >= args.limit:
            break
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
        if scanned % 500 == 0:
            print(f"  scanned={scanned} empty_matched={len(rows)}", flush=True)
        time.sleep(0.05)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_category_mapping_dry_run_{args.shop}_{stamp}.csv"
    fields = list(rows[0].keys()) if rows else [
        "product_id",
        "handle",
        "title",
        "status",
        "product_type",
        "tags",
        "current_category",
        "proposed_category",
        "proposed_bucket",
        "source",
    ]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    print(f"\nWrote {out}", flush=True)
    print(f"Scanned products: {scanned}", flush=True)
    print(f"Rows in report: {len(rows)}", flush=True)
    print("By source:", dict(source_counts), flush=True)
    print("By bucket:", dict(bucket_counts), flush=True)


if __name__ == "__main__":
    main()
