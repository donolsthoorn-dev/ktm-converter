#!/usr/bin/env python3
"""
Importeer nieuwste shopify_export_delta_*.csv via Admin API (geen Admin UI).

- Skip handles die al in Shopify bestaan (zoals “niet overschrijven”).
- Create via productSet + productCreateMedia (Image Src-URL’s).

  python3 scripts/shopify_import_delta_products.py --brands ktm,hsq,wp
  python3 scripts/shopify_import_delta_products.py --brands hsq --dry-run --limit 5
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

import config  # noqa: E402
from modules.shopify_client import get_shopify_products_index  # noqa: E402

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    raise SystemExit(1)

_PRODUCTS_DIR = {
    "ktm": ROOT / "output" / "products",
    "hsq": ROOT / "output" / "hsq" / "products",
    "wp": ROOT / "output" / "wp" / "products",
}

_GQL_PRODUCT_SET = """
mutation KtmProductSet($input: ProductSetInput!, $synchronous: Boolean!) {
  productSet(input: $input, synchronous: $synchronous) {
    product { id handle }
    userErrors { field message code }
  }
}
"""

_GQL_CREATE_MEDIA = """
mutation KtmProductCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
  productCreateMedia(productId: $productId, media: $media) {
    media { id status }
    mediaUserErrors { field message code }
  }
}
"""

_GQL_BY_HANDLE = """
query ($q: String!) {
  products(first: 1, query: $q) {
    nodes { id handle }
  }
}
"""


def _latest_delta(brand: str) -> Path | None:
    folder = _PRODUCTS_DIR[brand]
    paths = glob.glob(str(folder / "shopify_export_delta_*.csv"))
    if not paths:
        return None
    return Path(max(paths, key=os.path.getmtime))


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, query: str, variables: dict | None = None) -> dict:
    url = (
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"
    )
    r = sess.post(
        url,
        json={"query": query, "variables": variables or {}},
        headers={
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        },
        timeout=(15, 120),
    )
    r.raise_for_status()
    return r.json()


def _parse_delta_groups(path: Path) -> list[dict[str, Any]]:
    """Groepeer CSV-rijen per Handle tot product+varianten+images."""
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    by_handle: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        h = (row.get("Handle") or "").strip()
        if h:
            by_handle[h].append(row)

    groups: list[dict[str, Any]] = []
    for handle, items in by_handle.items():
        title_row = next((r for r in items if (r.get("Title") or "").strip()), items[0])
        images: list[str] = []
        seen_img: set[str] = set()
        for r in items:
            src = (r.get("Image Src") or "").strip()
            if src and src not in seen_img:
                seen_img.add(src)
                images.append(src)

        variants: list[dict[str, str]] = []
        for r in items:
            sku = (r.get("Variant SKU") or "").strip()
            opt_val = (r.get("Option1 Value") or "").strip()
            # Image-only extra rows hebben vaak geen SKU
            if not sku and not opt_val:
                continue
            if not sku and opt_val:
                # skip pure image rows that somehow have option
                if not (r.get("Variant Price") or "").strip():
                    continue
            variants.append(
                {
                    "sku": sku,
                    "barcode": (r.get("Variant Barcode") or "").strip(),
                    "price": (r.get("Variant Price") or "").strip() or "0.00",
                    "inventory_policy": (
                        (r.get("Variant Inventory Policy") or "continue").strip().lower()
                    ),
                    "option1_name": (r.get("Option1 Name") or "Title").strip() or "Title",
                    "option1_value": opt_val or "Default Title",
                }
            )

        if not variants or not images:
            continue

        published = (title_row.get("Published") or "TRUE").strip().upper() == "TRUE"
        groups.append(
            {
                "handle": handle,
                "title": (title_row.get("Title") or handle).strip(),
                "body_html": title_row.get("Body (HTML)") or "",
                "vendor": (title_row.get("Vendor") or "").strip(),
                "product_type": (title_row.get("Type") or "").strip(),
                "published": published,
                "images": images,
                "variants": variants,
            }
        )
    return groups


def _handle_exists(
    sess: requests.Session,
    handle: str,
    index: dict,
    *,
    live_check: bool,
) -> bool:
    if handle in index:
        return True
    if not live_check:
        return False
    body = _gql(sess, _GQL_BY_HANDLE, {"q": f"handle:{handle}"})
    nodes = (((body.get("data") or {}).get("products") or {}).get("nodes")) or []
    return bool(nodes)


def _create_product(sess: requests.Session, group: dict[str, Any]) -> tuple[str | None, str]:
    variants = group["variants"]
    opt_name = variants[0]["option1_name"]
    values: list[dict[str, str]] = []
    seen: set[str] = set()
    for v in variants:
        val = v["option1_value"]
        if val not in seen:
            seen.add(val)
            values.append({"name": val})

    set_variants = []
    for v in variants:
        policy = (v.get("inventory_policy") or "continue").upper()
        if policy not in ("CONTINUE", "DENY"):
            policy = "CONTINUE"
        set_variants.append(
            {
                "optionValues": [{"optionName": opt_name, "name": v["option1_value"]}],
                "sku": v.get("sku") or "",
                "barcode": v.get("barcode") or "",
                "price": v.get("price") or "0.00",
                "inventoryPolicy": policy,
            }
        )

    inp = {
        "title": group["title"],
        "handle": group["handle"],
        "descriptionHtml": group.get("body_html") or "",
        "vendor": group.get("vendor") or "",
        "productType": group.get("product_type") or "",
        "status": "ACTIVE" if group.get("published") else "DRAFT",
        "productOptions": [{"name": opt_name, "values": values or [{"name": "Default Title"}]}],
        "variants": set_variants,
    }
    body = _gql(sess, _GQL_PRODUCT_SET, {"input": inp, "synchronous": True})
    if body.get("errors"):
        return None, str(body.get("errors"))[:400]
    payload = ((body.get("data") or {}).get("productSet")) or {}
    uerr = payload.get("userErrors") or []
    if uerr:
        return None, str(uerr)[:400]
    prod = payload.get("product") or {}
    gid = prod.get("id") or ""
    pid = gid.rsplit("/", 1)[-1] if gid else ""
    if not pid:
        return None, "no product id"
    return pid, ""


def _attach_images(sess: requests.Session, product_id: str, urls: list[str]) -> str:
    urls = [u for u in urls if u.startswith(("http://", "https://"))][:20]
    if not urls:
        return "no_urls"
    media = [{"originalSource": u, "mediaContentType": "IMAGE"} for u in urls]
    body = _gql(
        sess,
        _GQL_CREATE_MEDIA,
        {"productId": f"gid://shopify/Product/{product_id}", "media": media},
    )
    if body.get("errors"):
        return str(body.get("errors"))[:300]
    payload = ((body.get("data") or {}).get("productCreateMedia")) or {}
    uerr = payload.get("mediaUserErrors") or []
    if uerr:
        return str(uerr)[:300]
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brands", default="ktm,hsq,wp")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="Max nieuwe producten (0=alle)")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument(
        "--live-handle-check",
        action="store_true",
        help="Bij cache-miss: GraphQL handle-check (trager, zekerder)",
    )
    ap.add_argument("--refresh-cache", action="store_true")
    args = ap.parse_args()

    brands = [b.strip().lower() for b in args.brands.split(",") if b.strip()]
    if args.refresh_cache:
        os.environ["KTM_FORCE_REFRESH_SHOPIFY_CACHE"] = "1"

    print("Shopify products-index laden…", flush=True)
    index = get_shopify_products_index() or {}
    print(f"Handles in cache: {len(index)}", flush=True)

    sess = _session()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_delta_import_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    created = skipped = failed = 0
    done = 0

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["brand", "handle", "title", "result", "product_id", "error"],
        )
        w.writeheader()

        for brand in brands:
            path = _latest_delta(brand)
            if not path:
                print(f"{brand}: geen delta-CSV", flush=True)
                continue
            print(f"\n=== {brand}: {path.name} ===", flush=True)
            groups = _parse_delta_groups(path)
            print(f"Productgroepen in CSV: {len(groups)}", flush=True)

            for g in groups:
                if args.limit and done >= args.limit:
                    break
                handle = g["handle"]
                if _handle_exists(
                    sess, handle, index, live_check=args.live_handle_check
                ):
                    skipped += 1
                    w.writerow(
                        {
                            "brand": brand,
                            "handle": handle,
                            "title": g["title"],
                            "result": "skipped_exists",
                            "product_id": "",
                            "error": "",
                        }
                    )
                    continue

                if args.dry_run:
                    created += 1
                    done += 1
                    w.writerow(
                        {
                            "brand": brand,
                            "handle": handle,
                            "title": g["title"],
                            "result": "dry_run_would_create",
                            "product_id": "",
                            "error": "",
                        }
                    )
                    print(f"  [dry-run] {handle}", flush=True)
                    continue

                pid, err = _create_product(sess, g)
                if not pid:
                    failed += 1
                    w.writerow(
                        {
                            "brand": brand,
                            "handle": handle,
                            "title": g["title"],
                            "result": "fail_create",
                            "product_id": "",
                            "error": err,
                        }
                    )
                    print(f"  FAIL create {handle}: {err}", flush=True)
                    time.sleep(args.sleep)
                    continue

                img_err = _attach_images(sess, pid, g["images"])
                result = "ok" if not img_err else "ok_media_warn"
                created += 1
                done += 1
                index[handle] = {"id": pid}
                w.writerow(
                    {
                        "brand": brand,
                        "handle": handle,
                        "title": g["title"],
                        "result": result,
                        "product_id": pid,
                        "error": img_err,
                    }
                )
                print(f"  OK {handle} id={pid} media={img_err or 'ok'}", flush=True)
                time.sleep(args.sleep)

            if args.limit and done >= args.limit:
                break

    print(
        f"\nKlaar: created={created} skipped={skipped} failed={failed}\nRapport: {out}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
