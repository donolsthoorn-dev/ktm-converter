#!/usr/bin/env python3
"""
ktm-shop.nl: SEO-titel, meta-omschrijving en alt-tekst.

Titel en alt zijn vaste regels. De meta-omschrijving komt van hetzelfde model
als de Motox e-bihr-job, zonder "Past op". HOMNN in de producttekst blijft staan.
Barcode blijft bij ktm_shopify_gtin_fill.

Alleen lege velden (fill-if-empty). Zonder --apply geen writes.

  python3 scripts/ktm_seo.py --limit 5
  python3 scripts/ktm_seo.py --limit 5 --apply
  python3 scripts/ktm_seo.py --handle 46301996000 --apply

Cache: cache/ktm/seo_completeness_descriptions.json
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import os
import sys
import time
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

import config  # noqa: E402
from modules.seo_completeness import (  # noqa: E402
    propose_image_alt,
    propose_seo_title,
    strip_homnn_plain,
    strip_html,
)

_motox_seo_path = ROOT / "scripts" / "motox_ebihr_seo_descriptions.py"
_spec = importlib.util.spec_from_file_location("motox_ebihr_seo_descriptions", _motox_seo_path)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"Kan {_motox_seo_path} niet laden")
motox_seo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(motox_seo)

CACHE_PATH = ROOT / "cache" / "ktm" / "seo_completeness_descriptions.json"

_QUERY_PAGE = """
query ($c: String, $q: String!) {
  products(first: 40, after: $c, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      vendor
      productType
      status
      descriptionHtml
      onlineStoreUrl
      seo { title description }
      media(first: 10) { nodes { id alt } }
    }
  }
}
"""

_QUERY_HANDLE = """
query ($h: String!) {
  productByHandle(handle: $h) {
    id
    handle
    title
    vendor
    productType
    status
    descriptionHtml
    onlineStoreUrl
    seo { title description }
    media(first: 10) { nodes { id alt } }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _product_row(p: dict | None) -> dict | None:
    if not p or (p.get("status") or "").upper() == "ARCHIVED":
        return None
    seo = (p.get("seo") or {}) or {}
    media = []
    for node in (p.get("media") or {}).get("nodes") or []:
        if not isinstance(node, dict) or not node.get("id"):
            continue
        media.append({"id": node["id"], "alt": (node.get("alt") or "").strip()})
    return {
        "id": p.get("id") or "",
        "handle": p.get("handle") or "",
        "title": p.get("title") or "",
        "vendor": p.get("vendor") or "",
        "type": p.get("productType") or "",
        "status": p.get("status") or "",
        "body_html": p.get("descriptionHtml") or "",
        "seo_title": (seo.get("title") or "").strip(),
        "seo_description": (seo.get("description") or "").strip(),
        "media": media,
        "url": p.get("onlineStoreUrl") or "",
    }


def _needs_alt(row: dict) -> bool:
    media = row.get("media") or []
    return bool(media) and all(not m["alt"] for m in media)


def _needs_seo(row: dict) -> bool:
    return not row["seo_title"] or not row["seo_description"] or _needs_alt(row)


def iter_products(sess, url, token, *, handles: list[str], limit: int, batch: str):
    yielded = 0
    target = None if limit <= 0 else limit
    seen: set[str] = set()

    def _take(row: dict | None) -> bool:
        nonlocal yielded
        if row is None or row["id"] in seen or not _needs_seo(row):
            return False
        seen.add(row["id"])
        yielded += 1
        return True

    if handles:
        for handle in handles:
            if target is not None and yielded >= target:
                return
            body = motox_seo._gql(sess, url, token, _QUERY_HANDLE, {"h": handle})
            row = _product_row(((body.get("data") or {}).get("productByHandle")))
            if row is None:
                print(f"Overgeslagen {handle} (niet gevonden of gearchiveerd)", flush=True)
                continue
            if not _needs_seo(row):
                print(f"Overgeslagen {handle} (SEO-titel, omschrijving en alt bestaan al)", flush=True)
                continue
            yielded += 1
            yield row
        return

    for query in motox_seo._BATCH_QUERIES[batch]:
        cursor = None
        while target is None or yielded < target:
            body = motox_seo._gql(sess, url, token, _QUERY_PAGE, {"c": cursor, "q": query})
            conn = ((body.get("data") or {}).get("products")) or {}
            for node in conn.get("nodes") or []:
                if target is not None and yielded >= target:
                    return
                row = _product_row(node)
                if _take(row):
                    yield row
            page = conn.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
            time.sleep(0.2)
        if target is not None and yielded >= target:
            return


def _describe(row: dict, *, call, model: str, cache: dict) -> tuple[str, str]:
    if row["seo_description"]:
        return row["seo_description"], "bestaand"
    plain = strip_homnn_plain(strip_html(row["body_html"]))[:700]
    payload = {
        "v": motox_seo.PROMPT_VERSION,
        "model": model,
        "title": row["title"],
        "vendor": row["vendor"],
        "type": row["type"],
        "body": plain,
    }
    items: dict = cache.setdefault("items", {})
    digest = motox_seo._source_hash(payload)
    cached = items.get(digest) or {}
    if cached.get("description"):
        return cached["description"], "cache"
    description, bron = motox_seo.propose_description(
        title=row["title"],
        vendor=row["vendor"] or "KTM",
        product_type=row["type"],
        body_html=plain,
        ymm="",
        model=model,
        call=call,
    )
    if bron == "ai" and description:
        items[digest] = {"description": description, "handle": row["handle"], "model": model}
        motox_seo._save_cache(CACHE_PATH, cache)
    return description, bron


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=5, help="Aantal producten (0 = alle lege)")
    ap.add_argument("--handle", action="append", default=[], help="Alleen deze handle (herhaalbaar)")
    ap.add_argument(
        "--batch",
        choices=tuple(motox_seo._BATCH_QUERIES),
        default="priority",
        help="priority = eerst voorraad, daarna de rest",
    )
    ap.add_argument("--apply", action="store_true", help="Schrijf SEO-titel, omschrijving en alt")
    ap.add_argument("--sleep", type=float, default=0.3, help="Pauze tussen AI-aanroepen")
    args = ap.parse_args()

    api_key, model, base_url = motox_seo._openai_settings()
    if not api_key:
        print("OPENAI_API_KEY ontbreekt. Zet die in .env (niet committen).", file=sys.stderr)
        return 2

    shop = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN or "").strip()
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or config.SHOPIFY_ACCESS_TOKEN or "").strip()
    api = (
        os.environ.get("SHOPIFY_ADMIN_API_VERSION") or config.SHOPIFY_ADMIN_API_VERSION or "2024-10"
    ).strip()
    if not shop or not token:
        print("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt.", file=sys.stderr)
        return 1

    shop_url = f"https://{shop}/admin/api/{api}/graphql.json"
    sess = _session()
    cache = motox_seo._load_cache(CACHE_PATH)

    def call(user: str) -> str:
        return motox_seo._call_model(
            sess, base_url=base_url, api_key=api_key, model=model, user=user
        )

    mode = "APPLY" if args.apply else "dry-run"
    print(f"{mode}  shop={shop}  model={model}  batch={args.batch}  limit={args.limit}", flush=True)

    rows: list[dict] = []
    written = 0
    for product in iter_products(
        sess,
        shop_url,
        token,
        handles=args.handle,
        limit=args.limit,
        batch=args.batch,
    ):
        seo_title = product["seo_title"] or propose_seo_title(product["title"], product["vendor"] or "KTM")
        description, bron = _describe(product, call=call, model=model, cache=cache)
        if bron not in ("bestaand", "cache"):
            time.sleep(args.sleep)
        alt = propose_image_alt(product["title"], product["vendor"] or "KTM")
        alt_targets = list(product["media"]) if _needs_alt(product) else []

        fout = ""
        if args.apply:
            if not product["seo_title"] or (description and not product["seo_description"]):
                fout = motox_seo._write_seo(
                    sess,
                    shop_url,
                    token,
                    product["id"],
                    seo_title if not product["seo_title"] else "",
                    description if description and not product["seo_description"] else "",
                )
            if not fout and alt_targets:
                fout = motox_seo._write_alts(
                    sess,
                    shop_url,
                    token,
                    product["id"],
                    [{"id": m["id"], "alt": alt} for m in alt_targets],
                )
            if not fout:
                written += 1
            time.sleep(0.25)

        status = "geschreven" if args.apply and not fout else ("fout" if fout else "voorstel")
        print(
            f"\n[{status}] {product['title']}\n"
            f"  handle {product['handle']}  titel {len(seo_title)} tekens\n"
            f"  {seo_title}\n"
            f"  omschrijving {len(description)} tekens  {bron}\n"
            f"  {description}\n"
            f"  alt {len(alt_targets)} beelden",
            flush=True,
        )
        if fout:
            print(f"  fout: {fout}", flush=True)
        rows.append(
            {
                "product_id": product["id"].rsplit("/", 1)[-1],
                "handle": product["handle"],
                "titel": product["title"],
                "vendor": product["vendor"],
                "huidige_seo_titel": product["seo_title"],
                "voorstel_titel": seo_title,
                "huidige_seo": product["seo_description"],
                "voorstel": description,
                "tekens": len(description),
                "alt": alt if alt_targets else "",
                "alt_beelden": len(alt_targets),
                "bron": bron,
                "fout": fout,
                "url": product["url"],
            }
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"ktm_seo_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["handle"]
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)

    print(f"Producten: {len(rows)}  geschreven: {written if args.apply else 0}", flush=True)
    print(f"CSV: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
