#!/usr/bin/env python3
"""
Vul lege Shopify Category op producten (Type → tags → titel/body → default).

  # ktm-shop.nl — alle producten zonder category (~22k)
  python3 scripts/shopify_category_apply.py --shop ktm --yes

  # Motox
  python3 scripts/shopify_category_apply.py --shop motox --yes

  # Test / hervatten
  python3 scripts/shopify_category_apply.py --shop ktm --limit 20 --yes
  python3 scripts/shopify_category_apply.py --shop ktm --yes --from-csv output/shopify_category_mapping_dry_run_ktm_….csv

Zonder --yes: dry-run (geen writes).
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
    DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    is_missing_shopify_category,
    resolve_shopify_product_category,
)

_REQUEST_TIMEOUT = (15, 120)

_QUERY_PRODUCTS = """
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

_SEARCH_TAXONOMY = """
query ($q: String!) {
  taxonomy {
    categories(first: 15, search: $q) {
      nodes { id name fullName }
    }
  }
}
"""

_MUT_UPDATE = """
mutation ($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product { id handle category { fullName } }
    userErrors { field message }
  }
}
"""


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
    """
    Creds uit omgeving (CI) of .env / .env.motox (lokaal).

    Motox in GitHub Actions:
      SHOPIFY_MOTOX_ACCESS_TOKEN, SHOPIFY_MOTOX_SHOP_DOMAIN
      optioneel SHOPIFY_MOTOX_ADMIN_API_VERSION
    Of zet gewoon SHOPIFY_ACCESS_TOKEN + SHOPIFY_SHOP_DOMAIN in de job-step.
    """
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
        raise SystemExit(
            f"Shopify creds ontbreken voor shop={shop} "
            f"(token/domain via env of .env[.motox])."
        )
    return domain, token, api


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


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


def resolve_taxonomy_gid(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    path: str,
    cache: dict[str, str | None],
) -> str | None:
    if path in cache:
        return cache[path]
    leaf = path.split(" > ")[-1]
    for query in (path, leaf):
        body = _gql(sess, shop, token, api, _SEARCH_TAXONOMY, {"q": query})
        nodes = (
            (((body.get("data") or {}).get("taxonomy") or {}).get("categories") or {}).get(
                "nodes"
            )
            or []
        )
        for n in nodes:
            if (n.get("fullName") or "") == path:
                cache[path] = n["id"]
                return n["id"]
        for n in nodes:
            fn = n.get("fullName") or ""
            if fn.endswith(leaf) or leaf.lower() in fn.lower():
                # Prefer exact leaf name match
                if n.get("name") == leaf or fn.endswith(" > " + leaf) or fn == leaf:
                    cache[path] = n["id"]
                    return n["id"]
    # Fallback: Motor Vehicle Parts parent
    if path != DEFAULT_SHOPIFY_PRODUCT_CATEGORY:
        gid = resolve_taxonomy_gid(
            sess, shop, token, api, DEFAULT_SHOPIFY_PRODUCT_CATEGORY, cache
        )
        cache[path] = gid
        return gid
    cache[path] = None
    return None


def iter_empty_products(sess, shop, token, api, limit: int):
    cursor = None
    scanned = 0
    while True:
        body = _gql(sess, shop, token, api, _QUERY_PRODUCTS, {"c": cursor})
        conn = body["data"]["products"]
        for p in conn["nodes"]:
            scanned += 1
            cat = (p.get("category") or {}).get("fullName") if p.get("category") else None
            if not is_missing_shopify_category(cat):
                continue
            yield p
            if limit and scanned >= limit:
                # limit is on scanned products not empties — keep going until enough yields?
                pass
        if limit and scanned >= limit:
            return
        if not conn["pageInfo"]["hasNextPage"]:
            return
        cursor = conn["pageInfo"]["endCursor"]
        time.sleep(0.04)


def products_from_csv(path: Path, limit: int):
    """Hergebruik dry-run CSV; resolve opnieuw met huidige mapper (zonder body)."""
    n = 0
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not is_missing_shopify_category(row.get("current_category")):
                continue
            n += 1
            yield {
                "id": f"gid://shopify/Product/{row['product_id']}",
                "handle": row.get("handle"),
                "title": row.get("title"),
                "status": row.get("status"),
                "productType": row.get("product_type") or "",
                "tags": [t.strip() for t in (row.get("tags") or "").split(",") if t.strip()],
                "descriptionHtml": "",
            }
            if limit and n >= limit:
                return


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument("--yes", action="store_true", help="Schrijf naar Shopify")
    ap.add_argument("--limit", type=int, default=0, help="Max lege producten (0=alle)")
    ap.add_argument(
        "--from-csv",
        type=Path,
        help="Optioneel: dry-run CSV i.p.v. live scan (sneller start)",
    )
    ap.add_argument("--sleep", type=float, default=0.12, help="Pauze tussen updates (s)")
    args = ap.parse_args()

    shop, token, api = _shop_credentials(args.shop)
    sess = _session()

    mode = "APPLY" if args.yes else "DRY-RUN"
    print(f"{mode} shop={shop} ({args.shop})", flush=True)

    if args.from_csv:
        if not args.from_csv.exists():
            raise SystemExit(f"CSV niet gevonden: {args.from_csv}")
        products = products_from_csv(args.from_csv, args.limit)
        print(f"Bron: {args.from_csv}", flush=True)
    else:
        products = iter_empty_products(sess, shop, token, api, 0)
        print("Bron: live products zonder category", flush=True)

    gid_cache: dict[str, str | None] = {}
    ok = fail = skip = 0
    source_counts: Counter[str] = Counter()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_category_apply_{args.shop}_{stamp}.csv"
    fields = [
        "product_id",
        "handle",
        "title",
        "proposed_category",
        "source",
        "bucket",
        "result",
        "applied_category",
    ]

    done = 0
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for p in products:
            if args.limit and done >= args.limit:
                break
            decision = resolve_shopify_product_category(
                product_type=p.get("productType"),
                tags=p.get("tags") or [],
                title=p.get("title"),
                body_html=p.get("descriptionHtml"),
            )
            source_counts[decision.source.split(":")[0]] += 1
            pid = (p.get("id") or "").split("/")[-1]
            row = {
                "product_id": pid,
                "handle": p.get("handle"),
                "title": p.get("title"),
                "proposed_category": decision.path,
                "source": decision.source,
                "bucket": decision.bucket,
                "result": "",
                "applied_category": "",
            }
            if not args.yes:
                row["result"] = "dry-run"
                w.writerow(row)
                done += 1
                ok += 1
                if done % 500 == 0:
                    print(f"  dry-run {done}…", flush=True)
                continue

            gid = resolve_taxonomy_gid(sess, shop, token, api, decision.path, gid_cache)
            if not gid:
                row["result"] = "no-taxonomy-gid"
                fail += 1
                w.writerow(row)
                done += 1
                continue

            body = _gql(
                sess,
                shop,
                token,
                api,
                _MUT_UPDATE,
                {"product": {"id": p["id"] if str(p["id"]).startswith("gid://") else f"gid://shopify/Product/{pid}", "category": gid}},
            )
            payload = ((body.get("data") or {}).get("productUpdate")) or {}
            errs = payload.get("userErrors") or []
            if errs:
                row["result"] = str(errs)
                fail += 1
            else:
                cat = ((payload.get("product") or {}).get("category") or {}).get("fullName") or ""
                row["result"] = "ok"
                row["applied_category"] = cat
                ok += 1
            w.writerow(row)
            done += 1
            if done % 100 == 0:
                print(f"  {done} ok={ok} fail={fail}", flush=True)
            time.sleep(args.sleep)

    print(f"\nWrote {out}", flush=True)
    print(f"Done: {done}  ok={ok} fail={fail} skip={skip}", flush=True)
    print("Sources:", dict(source_counts), flush=True)
    if not args.yes:
        print("Geen writes. Voeg --yes toe om toe te passen.", flush=True)


if __name__ == "__main__":
    main()
