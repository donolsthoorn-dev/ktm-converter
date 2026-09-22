#!/usr/bin/env python3
"""
Vul of herclassificeer Shopify Category op producten.

Lege fills (nachtelijke job / default) lezen de catalogus via één bulk-export:
  python3 scripts/shopify_category_apply.py --shop ktm --yes
  python3 scripts/shopify_category_apply.py --shop motox --yes
  python3 scripts/shopify_category_apply.py --shop ktm --limit 20 --yes
  python3 scripts/shopify_category_apply.py --shop ktm --yes --from-csv output/shopify_category_mapping_dry_run_ktm_….csv

Reclassify (goedgekeurde staging changes — overschrijft bestaande category):
  python3 scripts/shopify_category_apply.py --shop ktm --reclassify \\
    --from-csv output/shopify_category_reclassify_changes_ktm_….csv
  python3 scripts/shopify_category_apply.py --shop ktm --reclassify --yes \\
    --from-csv output/shopify_category_reclassify_changes_ktm_….csv \\
    --skip-done-csv output/shopify_category_reclassify_ktm_….csv \\
    --sleep 0.15

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
from modules.shopify_catalog_bulk import (  # noqa: E402
    download_product_catalog,
    iter_bulk_products,
)

_REQUEST_TIMEOUT = (15, 120)

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
    # Fallback: géén stille degrade naar Motor Vehicle Parts (dat maakte
    # fietsbatterijen/helmen etc. kapot). Liever fail dan verkeerde category.
    cache[path] = None
    return None


def iter_empty_products(sess, shop, token, api, label: str):
    """Producten zonder category, uit één bulk-export van de catalogus."""
    bulk_path = ROOT / "output" / f".category_fill_bulk_{label}_{datetime.now():%Y%m%d_%H%M%S}.jsonl"
    try:
        download_product_catalog(
            lambda query, variables=None: _gql(sess, shop, token, api, query, variables),
            bulk_path,
        )
        for p in iter_bulk_products(bulk_path):
            cat = (p.get("category") or {}).get("fullName") if p.get("category") else None
            if not is_missing_shopify_category(cat):
                continue
            yield p
    finally:
        bulk_path.unlink(missing_ok=True)


def products_from_csv(
    path: Path,
    limit: int,
    *,
    reclassify: bool = False,
    skip_ids: set[str] | None = None,
):
    """
    Hergebruik dry-run / staging CSV.

    - default: alleen rijen zonder category (of Uncategorized); mapper resolve opnieuw
    - reclassify: rijen met action change|set (of alle als geen action-kolom);
      gebruikt proposed_category uit de CSV (goedgekeurde staging)
    - skip_ids: product_id's overslaan (hervatten na Ctrl+C)
    """
    n = 0
    skipped = 0
    skip_ids = skip_ids or set()
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = (row.get("product_id") or "").strip()
            if pid and pid in skip_ids:
                skipped += 1
                continue
            if reclassify:
                action = (row.get("action") or "").strip().lower()
                if action and action not in ("change", "set"):
                    continue
                proposed = (row.get("proposed_category") or "").strip()
                if not proposed:
                    continue
            else:
                if not is_missing_shopify_category(row.get("current_category")):
                    continue
                proposed = ""
            n += 1
            yield {
                "id": f"gid://shopify/Product/{row['product_id']}",
                "handle": row.get("handle"),
                "title": row.get("title"),
                "status": row.get("status"),
                "productType": row.get("product_type") or "",
                "tags": [t.strip() for t in (row.get("tags") or "").split(",") if t.strip()],
                "descriptionHtml": "",
                "proposed_category": proposed,
                "source": (row.get("source") or "").strip(),
                "bucket": (row.get("proposed_bucket") or "").strip(),
            }
            if limit and n >= limit:
                break
    if skipped:
        print(f"Overgeslagen (al gedaan): {skipped}", flush=True)


def _load_done_ids(paths: list[Path]) -> set[str]:
    done: set[str] = set()
    for path in paths:
        if not path.exists():
            raise SystemExit(f"Skip-CSV niet gevonden: {path}")
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if (row.get("result") or "").strip() == "ok":
                    pid = (row.get("product_id") or "").strip()
                    if pid:
                        done.add(pid)
    return done


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument("--yes", action="store_true", help="Schrijf naar Shopify")
    ap.add_argument("--limit", type=int, default=0, help="Max producten (0=alle)")
    ap.add_argument(
        "--from-csv",
        type=Path,
        help="Optioneel: dry-run/staging CSV i.p.v. live scan",
    )
    ap.add_argument(
        "--reclassify",
        action="store_true",
        help="Overschrijf bestaande categories (vereist --from-csv met proposed_category)",
    )
    ap.add_argument(
        "--skip-done-csv",
        type=Path,
        action="append",
        default=[],
        help="Eerdere apply-rapport(en): product_ids met result=ok overslaan (hervatten)",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.15,
        help="Pauze tussen updates (s); hoger = minder Admin-druk",
    )
    args = ap.parse_args()

    if args.reclassify and not args.from_csv:
        raise SystemExit("--reclassify vereist --from-csv (goedgekeurde changes-CSV)")

    shop, token, api = _shop_credentials(args.shop)
    sess = _session()

    mode = "APPLY" if args.yes else "DRY-RUN"
    kind = "reclassify" if args.reclassify else "fill-empty"
    print(f"{mode} ({kind}) shop={shop} ({args.shop})", flush=True)

    skip_ids = _load_done_ids(args.skip_done_csv) if args.skip_done_csv else set()
    if skip_ids:
        print(f"Skip-lijst: {len(skip_ids)} al-ok product_ids", flush=True)

    if args.from_csv:
        if not args.from_csv.exists():
            raise SystemExit(f"CSV niet gevonden: {args.from_csv}")
        products = products_from_csv(
            args.from_csv,
            args.limit,
            reclassify=args.reclassify,
            skip_ids=skip_ids,
        )
        print(f"Bron: {args.from_csv}", flush=True)
    else:
        products = iter_empty_products(sess, shop, token, api, args.shop)
        print("Bron: bulk-export, producten zonder category", flush=True)

    gid_cache: dict[str, str | None] = {}
    ok = fail = skip = 0
    source_counts: Counter[str] = Counter()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "reclassify" if args.reclassify else "apply"
    out = ROOT / "output" / f"shopify_category_{suffix}_{args.shop}_{stamp}.csv"
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

            if args.reclassify and p.get("proposed_category"):
                path = p["proposed_category"]
                source = p.get("source") or "csv:proposed"
                bucket = p.get("bucket") or path.rsplit(" > ", 1)[-1]
            else:
                decision = resolve_shopify_product_category(
                    product_type=p.get("productType"),
                    tags=p.get("tags") or [],
                    title=p.get("title"),
                    body_html=p.get("descriptionHtml"),
                    shop=args.shop,
                )
                path = decision.path
                source = decision.source
                bucket = decision.bucket

            source_counts[source.split(":")[0]] += 1
            pid = (p.get("id") or "").split("/")[-1]
            row = {
                "product_id": pid,
                "handle": p.get("handle"),
                "title": p.get("title"),
                "proposed_category": path,
                "source": source,
                "bucket": bucket,
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

            gid = resolve_taxonomy_gid(sess, shop, token, api, path, gid_cache)
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
                {
                    "product": {
                        "id": p["id"]
                        if str(p["id"]).startswith("gid://")
                        else f"gid://shopify/Product/{pid}",
                        "category": gid,
                    }
                },
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
