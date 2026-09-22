#!/usr/bin/env python3
"""
Vul lege Shopify-variantbarcodes (GTIN) vanuit de KTM/HSQ/WP prijs-CSV's.

Alleen fill-if-empty: bestaande barcodes worden niet overschreven.
Eerste run is de inhaal over de hele catalogus; daarna blijft dit klein.
Doorlopende GTINs voor prijs/ETA-delta's gaan via ktm_price_eta_status_sync.

  python3 scripts/shopify_gtin_fill.py
  python3 scripts/shopify_gtin_fill.py --yes
  python3 scripts/shopify_gtin_fill.py --yes --limit 200 --sleep 0.15

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN, prijs-CSV's in input/.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
os.chdir(ROOT)

import config  # noqa: E402
from modules.pricing_loader import (  # noqa: E402
    load_barcode_index_from_35_z1_csv_files,
    lookup_in_str_index,
)
import shopify_sync_from_pricelist_csv as sync  # noqa: E402

_REQUEST_TIMEOUT = (15, 120)
QUERY = """
query ($c: String) {
  products(first: 50, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      status
      variants(first: 100) {
        nodes { id sku barcode }
      }
    }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, shop: str, token: str, api: str, variables: dict) -> dict:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    for attempt in range(8):
        r = sess.post(
            url,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            json={"query": QUERY, "variables": variables},
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


def _numeric_id(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--yes", action="store_true", help="Schrijf naar Shopify")
    ap.add_argument("--limit", type=int, default=0, help="Max te vullen varianten (0=alle)")
    ap.add_argument("--sleep", type=float, default=0.15, help="Pauze tussen product-bulkwrites")
    ap.add_argument(
        "--skip-done-csv",
        type=Path,
        default=None,
        help="CSV met kolom variant_id om al-gevulde rijen over te slaan",
    )
    args = ap.parse_args()

    sync.load_dotenv()
    shop = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN or "").strip()
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or config.SHOPIFY_ACCESS_TOKEN or "").strip()
    api = (
        os.environ.get("SHOPIFY_ADMIN_API_VERSION") or config.SHOPIFY_ADMIN_API_VERSION or "2024-10"
    ).strip()
    if args.yes and (not shop or not token):
        print("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt.", file=sys.stderr)
        return 1

    print("GTIN-index laden (KTM+HSQ+WP prijs-CSV)…", flush=True)
    barcode_index = load_barcode_index_from_35_z1_csv_files(project_root=str(ROOT))
    print(f"{len(barcode_index)} GTINs in prijsbestanden.", flush=True)

    skip_vids: set[str] = set()
    if args.skip_done_csv and args.skip_done_csv.is_file():
        with args.skip_done_csv.open(encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh, delimiter=";")
            for row in reader:
                vid = (row.get("variant_id") or "").strip()
                if vid and (row.get("result") or "").strip() == "ok":
                    skip_vids.add(vid)
        print(f"Skip-lijst: {len(skip_vids)} al-ok variant_ids", flush=True)

    sess = _session()
    by_product: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
    scanned_products = 0
    scanned_variants = 0
    already_filled = 0
    no_source = 0
    cursor = None
    print(f"Shop: {shop}  apply={args.yes}", flush=True)

    while True:
        body = _gql(sess, shop, token, api, {"c": cursor})
        conn = body["data"]["products"]
        for p in conn["nodes"]:
            scanned_products += 1
            pid = _numeric_id(p.get("id") or "")
            handle = p.get("handle") or ""
            for v in (p.get("variants") or {}).get("nodes") or []:
                scanned_variants += 1
                vid = _numeric_id(v.get("id") or "")
                sku = str(v.get("sku") or "").strip()
                cur = str(v.get("barcode") or "").strip()
                if cur:
                    already_filled += 1
                    continue
                if vid in skip_vids:
                    already_filled += 1
                    continue
                src = lookup_in_str_index(barcode_index, sku)
                if not src:
                    no_source += 1
                    continue
                by_product[pid].append((vid, src, sku, handle))
                if args.limit and sum(len(x) for x in by_product.values()) >= args.limit:
                    break
            if args.limit and sum(len(x) for x in by_product.values()) >= args.limit:
                break
        if args.limit and sum(len(x) for x in by_product.values()) >= args.limit:
            break
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
        time.sleep(0.05)

    todo = sum(len(v) for v in by_product.values())
    print(
        f"Gescand: {scanned_products} producten, {scanned_variants} varianten. "
        f"Al barcode: {already_filled}. Geen GTIN in CSV: {no_source}. Te vullen: {todo}.",
        flush=True,
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_gtin_fill_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["product_id", "variant_id", "handle", "sku", "gtin", "result"]
    ok = fail = 0
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        if not args.yes:
            for pid, items in by_product.items():
                for vid, gtin, sku, handle in items:
                    w.writerow(
                        {
                            "product_id": pid,
                            "variant_id": vid,
                            "handle": handle,
                            "sku": sku,
                            "gtin": gtin,
                            "result": "dry-run",
                        }
                    )
                    ok += 1
            print(f"Dry-run CSV: {out} ({ok} rijen). Geen Shopify-writes.", flush=True)
            return 0

        gql_sess = _session()
        done_products = 0
        for pid, items in by_product.items():
            for start in range(0, len(items), 100):
                chunk = items[start : start + 100]
                pairs = [(vid, gtin) for vid, gtin, _sku, _h in chunk]
                success, err = sync.graphql_product_variants_bulk_barcode(
                    shop, token, api, pid, pairs, sess=gql_sess
                )
                result = "ok" if success else str(err)[:500]
                if success:
                    ok += len(chunk)
                else:
                    fail += len(chunk)
                    print(f"Fout product {pid}: {result[:300]}", flush=True)
                for vid, gtin, sku, handle in chunk:
                    w.writerow(
                        {
                            "product_id": pid,
                            "variant_id": vid,
                            "handle": handle,
                            "sku": sku,
                            "gtin": gtin,
                            "result": result if not success else "ok",
                        }
                    )
            done_products += 1
            if done_products % 50 == 0:
                print(f"  {done_products}/{len(by_product)} producten  ok={ok} fail={fail}", flush=True)
            time.sleep(max(0.0, args.sleep))

    print(f"Klaar. ok={ok} fail={fail}  CSV: {out}", flush=True)
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
