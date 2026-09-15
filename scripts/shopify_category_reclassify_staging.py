#!/usr/bin/env python3
"""
Staging dry-run: huidige Shopify Category ↔ voorstel voor (bijna) alle producten.

Geen writes naar Shopify. Schrijft onder output/:
  - shopify_category_reclassify_staging_{shop}_{ts}.csv   (alles)
  - shopify_category_reclassify_changes_{shop}_{ts}.csv    (alleen verschillen)

  python3 scripts/shopify_category_reclassify_staging.py --shop ktm
  python3 scripts/shopify_category_reclassify_staging.py --shop ktm --limit 500
  python3 scripts/shopify_category_reclassify_staging.py --shop motox

Daarna mapper bijsturen op basis van changes-CSV; apply apart.
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

QUERY = """
query ($c: String) {
  products(first: 100, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      status
      productType
      tags
      descriptionHtml
      category { id fullName }
    }
  }
}
"""

FIELDS = [
    "product_id",
    "handle",
    "title",
    "status",
    "product_type",
    "tags",
    "current_category",
    "current_missing",
    "proposed_category",
    "proposed_bucket",
    "source",
    "action",  # keep | set | change
]


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _shop_credentials(shop: str) -> tuple[str, str, str]:
    if shop == "motox":
        token = (
            os.environ.get("SHOPIFY_MOTOX_ACCESS_TOKEN")
            or os.environ.get("SHOPIFY_ACCESS_TOKEN")
            or ""
        ).strip()
        domain = (
            os.environ.get("SHOPIFY_MOTOX_SHOP_DOMAIN")
            or os.environ.get("SHOPIFY_SHOP_DOMAIN")
            or ""
        ).strip()
        api = (
            os.environ.get("SHOPIFY_MOTOX_ADMIN_API_VERSION")
            or os.environ.get("SHOPIFY_ADMIN_API_VERSION")
            or ""
        ).strip()
        file_env = _load_dotenv(ROOT / ".env.motox")
    else:
        token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
        domain = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or "").strip()
        api = (os.environ.get("SHOPIFY_ADMIN_API_VERSION") or "").strip()
        file_env = _load_dotenv(ROOT / ".env")

    token = token or (file_env.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
    domain = domain or (file_env.get("SHOPIFY_SHOP_DOMAIN") or "").strip()
    api = api or (file_env.get("SHOPIFY_ADMIN_API_VERSION") or "2024-10").strip()
    if not token or not domain:
        raise SystemExit(f"Shopify creds ontbreken voor shop={shop}")
    return domain, token, api


def _gql(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    query: str,
    variables: dict | None = None,
) -> dict:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    for attempt in range(10):
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
        if r.status_code >= 500:
            time.sleep(2 + attempt)
            continue
        r.raise_for_status()
        body = r.json()
        if body.get("errors"):
            if "Throttl" in str(body["errors"]):
                time.sleep(2 + attempt)
                continue
            raise RuntimeError(body["errors"])
        return body
    raise RuntimeError("GraphQL failed after retries")


def _norm_cat(name: str | None) -> str:
    return (name or "").strip()


def _action(current: str, proposed: str) -> str:
    if is_missing_shopify_category(current):
        return "set"
    if _norm_cat(current) == _norm_cat(proposed):
        return "keep"
    return "change"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument("--limit", type=int, default=0, help="Max producten (0=alle)")
    args = ap.parse_args()

    shop_domain, token, api = _shop_credentials(args.shop)
    sess = requests.Session()
    sess.trust_env = False

    print(f"Staging reclassify dry-run shop={shop_domain} ({args.shop})", flush=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging_path = ROOT / "output" / f"shopify_category_reclassify_staging_{args.shop}_{stamp}.csv"
    changes_path = ROOT / "output" / f"shopify_category_reclassify_changes_{args.shop}_{stamp}.csv"

    action_counts: Counter[str] = Counter()
    bucket_change: Counter[str] = Counter()
    source_change: Counter[str] = Counter()
    scanned = 0

    with staging_path.open("w", newline="", encoding="utf-8") as f_all, changes_path.open(
        "w", newline="", encoding="utf-8"
    ) as f_chg:
        w_all = csv.DictWriter(f_all, fieldnames=FIELDS)
        w_chg = csv.DictWriter(f_chg, fieldnames=FIELDS)
        w_all.writeheader()
        w_chg.writeheader()

        cursor = None
        while True:
            body = _gql(sess, shop_domain, token, api, QUERY, {"c": cursor})
            conn = body["data"]["products"]
            for p in conn["nodes"]:
                scanned += 1
                current = _norm_cat(
                    (p.get("category") or {}).get("fullName") if p.get("category") else None
                )
                decision = resolve_shopify_product_category(
                    product_type=p.get("productType"),
                    tags=p.get("tags") or [],
                    title=p.get("title"),
                    body_html=p.get("descriptionHtml"),
                )
                action = _action(current, decision.path)
                row = {
                    "product_id": (p.get("id") or "").split("/")[-1],
                    "handle": p.get("handle") or "",
                    "title": p.get("title") or "",
                    "status": p.get("status") or "",
                    "product_type": p.get("productType") or "",
                    "tags": ", ".join(p.get("tags") or []),
                    "current_category": current,
                    "current_missing": "1" if is_missing_shopify_category(current) else "0",
                    "proposed_category": decision.path,
                    "proposed_bucket": decision.bucket,
                    "source": decision.source,
                    "action": action,
                }
                w_all.writerow(row)
                action_counts[action] += 1
                if action in ("set", "change"):
                    w_chg.writerow(row)
                    bucket_change[decision.bucket] += 1
                    source_change[decision.source.split(":")[0]] += 1

                if args.limit and scanned >= args.limit:
                    break
            if args.limit and scanned >= args.limit:
                break
            if not conn["pageInfo"]["hasNextPage"]:
                break
            cursor = conn["pageInfo"]["endCursor"]
            if scanned % 5000 == 0:
                print(
                    f"  scanned={scanned} keep={action_counts['keep']} "
                    f"change={action_counts['change']} set={action_counts['set']}",
                    flush=True,
                )
            time.sleep(0.04)

    print(flush=True)
    print(f"Wrote {staging_path}", flush=True)
    print(f"Wrote {changes_path}", flush=True)
    print(f"Scanned: {scanned}", flush=True)
    print(
        f"Actions: keep={action_counts['keep']}  "
        f"change={action_counts['change']}  set={action_counts['set']}",
        flush=True,
    )
    print("Changes by proposed bucket (top 15):", flush=True)
    for b, n in bucket_change.most_common(15):
        print(f"  {n:6}  {b}", flush=True)
    print("Changes by source:", dict(source_change), flush=True)
    print(
        "Geen Shopify-writes. Review changes-CSV → mapper bijsturen → daarna apply.",
        flush=True,
    )


if __name__ == "__main__":
    main()
