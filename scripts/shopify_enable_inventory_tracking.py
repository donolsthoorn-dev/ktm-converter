#!/usr/bin/env python3
"""
Zet inventoryItem.tracked=true op varianten (bijv. na API-import zonder tracker).

  python3 scripts/shopify_enable_inventory_tracking.py --from-import-report output/shopify_delta_import_….csv
  python3 scripts/shopify_enable_inventory_tracking.py --product-id 16103847854410
  python3 scripts/shopify_enable_inventory_tracking.py --from-import-report … --dry-run
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

import config  # noqa: E402

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    raise SystemExit(1)

_GQL_VARIANTS = """
query ($id: ID!) {
  product(id: $id) {
    id
    handle
    variants(first: 100) {
      nodes {
        id
        sku
        inventoryItem { id tracked }
      }
    }
  }
}
"""

_GQL_TRACK = """
mutation ($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    inventoryItem { id tracked }
    userErrors { field message }
  }
}
"""


def _gql(sess: requests.Session, query: str, variables: dict) -> dict:
    url = (
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"
    )
    r = sess.post(
        url,
        json={"query": query, "variables": variables},
        headers={
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        },
        timeout=(15, 60),
    )
    r.raise_for_status()
    return r.json()


def _product_gid(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("gid://"):
        return s
    return f"gid://shopify/Product/{s}"


def _ids_from_report(path: Path) -> list[str]:
    out: list[str] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("result") not in ("ok", "ok_media_warn"):
                continue
            pid = (row.get("product_id") or "").strip()
            if pid:
                out.append(pid)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-import-report", type=Path, help="CSV van shopify_import_delta_products")
    ap.add_argument("--product-id", action="append", default=[], help="Shopify product id (herhaalbaar)")
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ids: list[str] = []
    if args.from_import_report:
        ids.extend(_ids_from_report(args.from_import_report))
    ids.extend(args.product_id or [])
    # unique preserve order
    seen: set[str] = set()
    product_ids = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            product_ids.append(i)

    if not product_ids:
        print("Geen product-ids. Gebruik --from-import-report of --product-id.", file=sys.stderr)
        return 2

    sess = requests.Session()
    sess.trust_env = False
    enabled = already = failed = 0

    for pid in product_ids:
        body = _gql(sess, _GQL_VARIANTS, {"id": _product_gid(pid)})
        if body.get("errors"):
            failed += 1
            print(f"FAIL query {pid}: {body.get('errors')}", flush=True)
            continue
        prod = ((body.get("data") or {}).get("product")) or {}
        handle = prod.get("handle") or pid
        nodes = (((prod.get("variants") or {}).get("nodes")) or [])
        if not nodes:
            print(f"SKIP {handle}: geen varianten", flush=True)
            continue
        for node in nodes:
            item = node.get("inventoryItem") or {}
            iid = item.get("id") or ""
            tracked = bool(item.get("tracked"))
            sku = node.get("sku") or ""
            if tracked:
                already += 1
                print(f"  already {handle} sku={sku}", flush=True)
                continue
            if args.dry_run:
                enabled += 1
                print(f"  [dry-run] would track {handle} sku={sku}", flush=True)
                continue
            upd = _gql(sess, _GQL_TRACK, {"id": iid, "input": {"tracked": True}})
            if upd.get("errors"):
                failed += 1
                print(f"FAIL {handle} sku={sku}: {upd.get('errors')}", flush=True)
                continue
            payload = ((upd.get("data") or {}).get("inventoryItemUpdate")) or {}
            uerr = payload.get("userErrors") or []
            if uerr:
                failed += 1
                print(f"FAIL {handle} sku={sku}: {uerr}", flush=True)
                continue
            enabled += 1
            print(f"  OK tracked {handle} sku={sku}", flush=True)
            time.sleep(args.sleep)

    print(
        f"\nKlaar: enabled={enabled} already={already} failed={failed} products={len(product_ids)}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
