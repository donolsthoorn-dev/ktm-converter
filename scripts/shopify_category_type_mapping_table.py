#!/usr/bin/env python3
"""
Exporteer alle unieke Shopify product Types → category mapping-tabel.

Doel: gaten in TYPE_EXACT / TYPE_PREFIX zichtbaar maken en handmatig bijsturen.

  # Live catalogus scannen (KTM)
  python3 scripts/shopify_category_type_mapping_table.py --shop ktm

  # Sneller: hergebruik laatste staging-CSV
  python3 scripts/shopify_category_type_mapping_table.py --shop ktm --from-staging

Output:
  output/shopify_category_type_map_{shop}_{ts}.csv

Kolommen o.a.:
  coverage = exact | prefix | generic | unmapped
  needs_review = 1 als type geen vaste type-hit heeft (generic/unmapped)
  manual_category = leeg, voor handmatige override in de CSV
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
os.chdir(ROOT)

from modules.category_mapper import (  # noqa: E402
    GENERIC_TYPES,
    TYPE_EXACT,
    TYPE_PREFIX,
    _BRAND_TYPE_PREFIX,
    _type_lookup_keys,
    resolve_shopify_product_category,
)

_REQUEST_TIMEOUT = (15, 120)

QUERY = """
query ($c: String) {
  products(first: 100, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes {
      handle
      title
      productType
      status
    }
  }
}
"""

FIELDS = [
    "product_type",
    "product_type_bare",
    "product_count",
    "coverage",
    "mapped_via",
    "type_only_category",
    "type_only_bucket",
    "type_only_source",
    "with_sample_category",
    "with_sample_bucket",
    "with_sample_source",
    "needs_review",
    "manual_category",
    "sample_handle",
    "sample_title",
    "sample_status",
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


def _bare_type(ptype: str) -> str:
    return _BRAND_TYPE_PREFIX.sub("", (ptype or "").strip().lower()).strip()


def _type_coverage(ptype: str, shop: str = "ktm") -> tuple[str, str]:
    """Return (coverage, mapped_via)."""
    keys = _type_lookup_keys((ptype or "").strip().lower())
    if not any(keys):
        return "unmapped", ""

    exact = TYPE_EXACT
    prefix_rules = TYPE_PREFIX
    generic = GENERIC_TYPES
    motox_exact: dict = {}
    if shop == "motox":
        from modules.category_mapper_motox import (  # noqa: WPS433
            GENERIC_TYPES_MOTOX,
            TYPE_EXACT_MOTOX,
            TYPE_PREFIX_MOTOX,
        )

        motox_exact = TYPE_EXACT_MOTOX
        exact = {**TYPE_EXACT, **TYPE_EXACT_MOTOX}
        prefix_rules = list(TYPE_PREFIX_MOTOX) + list(TYPE_PREFIX)
        generic = GENERIC_TYPES | GENERIC_TYPES_MOTOX

    if any(k in generic for k in keys):
        return "generic", "GENERIC_TYPES"
    for key in keys:
        if key in exact:
            via = "TYPE_EXACT_MOTOX" if key in motox_exact else "TYPE_EXACT"
            return "exact", f"{via}:{key}"
    for key in keys:
        for prefix, _path in prefix_rules:
            if key.startswith(prefix):
                return "prefix", f"TYPE_PREFIX:{prefix.strip()}"
    return "unmapped", ""


def _collect_from_live(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    limit: int,
) -> dict[str, dict]:
    """product_type → {count, sample_handle, sample_title, sample_status}."""
    stats: dict[str, dict] = {}
    cursor = None
    scanned = 0
    while True:
        body = _gql(sess, shop, token, api, QUERY, {"c": cursor})
        conn = body["data"]["products"]
        for p in conn["nodes"]:
            scanned += 1
            pt = (p.get("productType") or "").strip() or "(empty)"
            row = stats.get(pt)
            if not row:
                stats[pt] = {
                    "count": 1,
                    "sample_handle": p.get("handle") or "",
                    "sample_title": p.get("title") or "",
                    "sample_status": p.get("status") or "",
                }
            else:
                row["count"] += 1
            if limit and scanned >= limit:
                return stats
        if not conn["pageInfo"]["hasNextPage"]:
            return stats
        cursor = conn["pageInfo"]["endCursor"]
        if scanned % 5000 == 0:
            print(f"  scanned={scanned} unique_types={len(stats)}", flush=True)
        time.sleep(0.04)


def _latest_staging(shop: str) -> Path | None:
    paths = sorted(Path("output").glob(f"shopify_category_reclassify_staging_{shop}_*.csv"))
    return paths[-1] if paths else None


def _collect_from_staging(path: Path) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pt = (row.get("product_type") or "").strip() or "(empty)"
            cur = stats.get(pt)
            if not cur:
                stats[pt] = {
                    "count": 1,
                    "sample_handle": row.get("handle") or "",
                    "sample_title": row.get("title") or "",
                    "sample_status": row.get("status") or "",
                }
            else:
                cur["count"] += 1
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument("--limit", type=int, default=0, help="Max producten bij live scan (0=alle)")
    ap.add_argument(
        "--from-staging",
        action="store_true",
        help="Gebruik nieuwste staging-CSV i.p.v. live scan",
    )
    ap.add_argument(
        "--staging-csv",
        type=Path,
        help="Specifieke staging-CSV (impliciet --from-staging)",
    )
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_category_type_map_{args.shop}_{stamp}.csv"

    if args.staging_csv or args.from_staging:
        path = args.staging_csv or _latest_staging(args.shop)
        if not path or not path.exists():
            raise SystemExit("Geen staging-CSV gevonden. Draai eerst reclassify staging of geef --staging-csv.")
        print(f"Bron: staging {path}", flush=True)
        stats = _collect_from_staging(path)
    else:
        shop, token, api = _shop_credentials(args.shop)
        sess = requests.Session()
        sess.trust_env = False
        print(f"Bron: live scan shop={shop} ({args.shop})", flush=True)
        stats = _collect_from_live(sess, shop, token, api, args.limit)

    coverage_counts: dict[str, int] = defaultdict(int)
    review_n = 0

    rows_out: list[dict] = []
    for pt in sorted(stats.keys(), key=lambda t: (-stats[t]["count"], t.lower())):
        info = stats[pt]
        bare = _bare_type(pt) if pt != "(empty)" else ""
        coverage, mapped_via = _type_coverage("" if pt == "(empty)" else pt, shop=args.shop)

        type_only = resolve_shopify_product_category(
            product_type="" if pt == "(empty)" else pt,
            title="",
            tags=[],
            shop=args.shop,
        )
        with_sample = resolve_shopify_product_category(
            product_type="" if pt == "(empty)" else pt,
            title=info["sample_title"],
            tags=[],
            shop=args.shop,
        )

        needs = coverage in ("unmapped", "generic") or type_only.source.startswith("default")
        if needs:
            review_n += 1
        coverage_counts[coverage] += 1

        rows_out.append(
            {
                "product_type": pt,
                "product_type_bare": bare,
                "product_count": info["count"],
                "coverage": coverage,
                "mapped_via": mapped_via,
                "type_only_category": type_only.path,
                "type_only_bucket": type_only.bucket,
                "type_only_source": type_only.source,
                "with_sample_category": with_sample.path,
                "with_sample_bucket": with_sample.bucket,
                "with_sample_source": with_sample.source,
                "needs_review": "1" if needs else "0",
                "manual_category": "",
                "sample_handle": info["sample_handle"],
                "sample_title": info["sample_title"],
                "sample_status": info["sample_status"],
            }
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows_out)

    print(f"\nWrote {out}", flush=True)
    print(f"Unique types: {len(rows_out)}", flush=True)
    print(f"Coverage: {dict(coverage_counts)}", flush=True)
    print(f"Needs review: {review_n}", flush=True)
    print(
        "Vul manual_category waar nodig; daarna kunnen we die in TYPE_EXACT laden.",
        flush=True,
    )


if __name__ == "__main__":
    main()
