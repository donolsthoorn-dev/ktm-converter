#!/usr/bin/env python3
"""
Publiceer ACTIVE Shopify-producten op het Online Store-kanaal wanneer ze dat volgens ERP
zouden moeten zijn, maar published_at nog leeg is (Webshop UIT in admin).

Criteria per product:
  - status ACTIVE
  - published_at leeg
  - minstens één variant-SKU met ArticleStatus in CSV, en niet overal 80

Mutatie: GraphQL productUpdate met status ACTIVE + published: true
(werkt met bestaande product-scopes; geen read/write_publications nodig).

Standaard dry-run. Met --apply worden producten echt gepubliceerd.

Voorbeelden:
  python3 scripts/shopify_publish_sellable_active_products.py
  python3 scripts/shopify_publish_sellable_active_products.py --apply
  python3 scripts/shopify_publish_sellable_active_products.py --apply --limit 50
  python3 scripts/shopify_publish_sellable_active_products.py --input-csv output/active_not_on_webshop.csv

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN
Optioneel: ArticleStatus uit input/*35_Z1_EUR_EN_csv.csv (zelfde als andere sync-scripts)
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
    lookup_in_str_index,
    normalize_sku_key,
)

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)

_GQL_PUBLISH = """
mutation KtmPublishProduct($input: ProductInput!) {
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


def _product_is_sellable_per_csv(variants: list[dict], status_by_sku: dict[str, str]) -> bool:
    statuses = _variant_article_statuses(variants, status_by_sku)
    if not statuses:
        return False
    return any(st != "80" for st in statuses)


def _collect_candidates_rest(
    sess: requests.Session,
    status_by_sku: dict[str, str],
) -> list[dict]:
    headers = {"X-Shopify-Access-Token": TOKEN}
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        f"?limit=250&status=active&fields=id,handle,title,status,published_at,variants"
    )
    scanned = 0
    candidates: list[dict] = []

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
            if not _product_is_sellable_per_csv(variants, status_by_sku):
                continue
            skus = [normalize_sku_key(v.get("sku")) for v in variants if v.get("sku")]
            statuses = [
                lookup_in_str_index(status_by_sku, s).strip()
                for s in skus
            ]
            statuses = [s for s in statuses if s]
            candidates.append(
                {
                    "product_id": str(p.get("id") or ""),
                    "handle": (p.get("handle") or "").strip(),
                    "title": (p.get("title") or "").strip()[:120],
                    "skus": ",".join(skus[:5]) + ("..." if len(skus) > 5 else ""),
                    "article_statuses": ",".join(sorted(set(statuses))),
                    "variant_count": len(variants),
                }
            )

        if scanned % 5000 == 0 and scanned:
            print(
                f"  ... {scanned} ACTIVE gescand, kandidaten {len(candidates)}",
                flush=True,
            )
        url = _next_url_from_link_header(r.headers.get("Link"))
        time.sleep(0.2)

    print(f"ACTIVE gescand: {scanned}", flush=True)
    return candidates


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
                    "skus": (row.get("skus") or "").strip(),
                    "article_statuses": (row.get("article_statuses") or "").strip(),
                    "variant_count": (row.get("variant_count") or "").strip(),
                }
            )
    return rows


def _publish_product(sess: requests.Session, product_id: str) -> tuple[bool, str, str | None]:
    gid = f"gid://shopify/Product/{product_id}"
    body = _graphql_post(
        sess,
        _GQL_PUBLISH,
        {
            "input": {
                "id": gid,
                "status": "ACTIVE",
                "published": True,
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
    if not pub:
        return False, "publishedAt nog steeds leeg na productUpdate", None
    return True, "", str(pub)


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

    status_by_sku = load_article_status_from_35_z1_csv_files(config.INPUT_DIR)
    if status_by_sku:
        print(f"CSV ArticleStatus-index: {len(status_by_sku)} SKU's.", flush=True)
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

    sess = _http_session()

    if args.input_csv and args.input_csv.is_file():
        print(f"Kandidaten uit CSV: {args.input_csv}", flush=True)
        candidates = _load_candidates_from_csv(args.input_csv)
    else:
        print("Catalogusscan: ACTIVE + published_at leeg + ERP verkoopbaar...", flush=True)
        candidates = _collect_candidates_rest(sess, status_by_sku)

    candidates.sort(key=lambda r: (r.get("handle") or "").lower())
    _write_report(
        args.output_csv,
        candidates,
        ["product_id", "handle", "title", "skus", "article_statuses", "variant_count"],
    )
    print(f"Kandidaten: {len(candidates)} → {args.output_csv}", flush=True)

    if not args.apply:
        print("Dry-run: geen publicaties in Shopify.", flush=True)
        for row in candidates[:15]:
            print(
                f"  {row['handle']}: {row['title'][:50]} | {row['skus']} | ERP {row['article_statuses']}",
                flush=True,
            )
        if len(candidates) > 15:
            print(f"  ... +{len(candidates) - 15} meer in CSV", flush=True)
        return 0

    to_publish = candidates
    if args.limit > 0:
        to_publish = candidates[: args.limit]
        print(f"--limit {args.limit}: alleen eerste {len(to_publish)} producten.", flush=True)

    sleep_sec = float(
        (os.environ.get("SHOPIFY_PUBLISH_SLEEP_SEC") or "").strip() or "0.25"
    )
    progress_every = max(50, int(os.environ.get("SHOPIFY_PUBLISH_PROGRESS_EVERY", "100")))

    published_rows: list[dict] = []
    error_rows: list[dict] = []
    mirror_flush: list[tuple[str, str]] = []

    print(f"Publiceren: {len(to_publish)} producten...", flush=True)
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
        ["product_id", "handle", "title", "skus", "article_statuses", "variant_count", "published_at"],
    )
    _write_report(
        args.errors_csv,
        error_rows,
        ["product_id", "handle", "title", "skus", "article_statuses", "variant_count", "error"],
    )

    print(
        f"Klaar: gepubliceerd {len(published_rows)}, fouten {len(error_rows)}.",
        flush=True,
    )
    print(f"  Succes: {args.published_csv}", flush=True)
    if error_rows:
        print(f"  Fouten: {args.errors_csv}", flush=True)
    return 1 if error_rows else 0


if __name__ == "__main__":
    raise SystemExit(main())
