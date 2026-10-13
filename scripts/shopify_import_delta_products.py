#!/usr/bin/env python3
"""
Importeer nieuwste shopify_export_delta_*.csv via Admin API (geen Admin UI).

- Skip handles die al in Shopify bestaan (zoals “niet overschrijven”).
- Create via productSet + productCreateMedia (Image Src-URL’s).
- Inventory tracking aan, Product category uit CSV, publicatie op alle kanalen.

  python3 scripts/shopify_import_delta_products.py --brands ktm,hsq,wp
  python3 scripts/shopify_import_delta_products.py --brands hsq --dry-run --limit 5
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
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
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

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

_GQL_SEARCH_TAXONOMY = """
query ($q: String!) {
  taxonomy {
    categories(first: 15, search: $q) {
      nodes { id name fullName }
    }
  }
}
"""

_GQL_PRODUCT_UPDATE_CATEGORY = """
mutation ($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product { id category { fullName } }
    userErrors { field message }
  }
}
"""

_GQL_PUBLICATIONS = """
query {
  publications(first: 50) {
    nodes { id name }
  }
}
"""

_GQL_PUBLISH = """
mutation ($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

# Fallback zonder read/write_publications: alleen Online Store (legacy).
_GQL_PUBLISH_ONLINE = """
mutation ($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id publishedAt }
    userErrors { field message }
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
        tags_raw = (title_row.get("Tags") or "").strip()
        tags = [t.strip() for t in tags_raw.split(",") if t.strip()] if tags_raw else []
        groups.append(
            {
                "handle": handle,
                "title": (title_row.get("Title") or handle).strip(),
                "body_html": title_row.get("Body (HTML)") or "",
                "vendor": (title_row.get("Vendor") or "").strip(),
                "product_type": (title_row.get("Type") or "").strip(),
                "product_category": (title_row.get("Product category") or "").strip(),
                "tags": tags,
                "seo_title": (title_row.get("SEO Title") or "").strip(),
                "seo_description": (title_row.get("SEO Description") or "").strip(),
                "published": published,
                "images": images,
                "variants": variants,
            }
        )
    return groups


def _resolve_taxonomy_gid(
    sess: requests.Session,
    path: str,
    cache: dict[str, str | None],
) -> str | None:
    path = (path or "").strip()
    if not path:
        return None
    if path in cache:
        return cache[path]
    leaf = path.split(" > ")[-1].strip()
    for query in (path, leaf):
        body = _gql(sess, _GQL_SEARCH_TAXONOMY, {"q": query})
        if body.get("errors"):
            continue
        nodes = (
            (((body.get("data") or {}).get("taxonomy") or {}).get("categories") or {}).get(
                "nodes"
            )
            or []
        )
        for n in nodes:
            if (n.get("fullName") or "") == path:
                cache[path] = n.get("id")
                return cache[path]
        for n in nodes:
            fn = n.get("fullName") or ""
            if n.get("name") == leaf or fn.endswith(" > " + leaf) or fn == leaf:
                cache[path] = n.get("id")
                return cache[path]
    cache[path] = None
    return None


def _list_publication_ids(sess: requests.Session) -> list[str]:
    body = _gql(sess, _GQL_PUBLICATIONS)
    if body.get("errors"):
        print(f"  warn publications: {body.get('errors')}", flush=True)
        return []
    nodes = (((body.get("data") or {}).get("publications") or {}).get("nodes")) or []
    return [n["id"] for n in nodes if n.get("id")]


def _set_category(sess: requests.Session, product_id: str, category_gid: str) -> str:
    body = _gql(
        sess,
        _GQL_PRODUCT_UPDATE_CATEGORY,
        {
            "product": {
                "id": f"gid://shopify/Product/{product_id}",
                "category": category_gid,
            }
        },
    )
    if body.get("errors"):
        return str(body.get("errors"))[:300]
    payload = ((body.get("data") or {}).get("productUpdate")) or {}
    uerr = payload.get("userErrors") or []
    return str(uerr)[:300] if uerr else ""


def _publish_online_store_only(sess: requests.Session, product_id: str) -> str:
    """Legacy Online Store publish when publications scope ontbreekt."""
    body = _gql(
        sess,
        _GQL_PUBLISH_ONLINE,
        {"input": {"id": f"gid://shopify/Product/{product_id}", "published": True}},
    )
    if body.get("errors"):
        # Nieuwere API zonder ProductInput.published → REST fallback
        err = str(body.get("errors"))
        if "published" in err.lower() or "ProductInput" in err:
            return _publish_online_store_rest(sess, product_id) or err[:300]
        return err[:300]
    payload = ((body.get("data") or {}).get("productUpdate")) or {}
    uerr = payload.get("userErrors") or []
    if uerr:
        return str(uerr)[:300]
    return ""


def _publish_online_store_rest(sess: requests.Session, product_id: str) -> str:
    url = (
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/products/{product_id}.json"
    )
    r = sess.put(
        url,
        json={"product": {"id": int(product_id), "published": True}},
        headers={
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json",
        },
        timeout=(15, 60),
    )
    if r.status_code >= 400:
        return f"rest_publish:{r.status_code}:{r.text[:200]}"
    return ""


def _publish_channels(
    sess: requests.Session,
    product_id: str,
    publication_ids: list[str],
) -> str:
    if not publication_ids:
        err = _publish_online_store_only(sess, product_id)
        return err  # leeg = ok (Online Store only)
    body = _gql(
        sess,
        _GQL_PUBLISH,
        {
            "id": f"gid://shopify/Product/{product_id}",
            "input": [{"publicationId": pid} for pid in publication_ids],
        },
    )
    if body.get("errors"):
        err_s = str(body.get("errors"))
        if "ACCESS" in err_s.upper() or "publication" in err_s.lower():
            return _publish_online_store_only(sess, product_id)
        return err_s[:300]
    uerr = (((body.get("data") or {}).get("publishablePublish")) or {}).get("userErrors") or []
    return str(uerr)[:300] if uerr else ""


def _index_has_handle(index: dict, handle: str) -> bool:
    h = normalize_shopify_product_handle(handle)
    if not h:
        return False
    # Cache keys are normalize_shopify_product_handle()'d.
    return h in index or handle in index


def _handle_search_query(handle: str) -> str:
    h = (handle or "").strip()
    if not h:
        return ""
    if any(c in h for c in ' \t"\\:'):
        esc = h.replace("\\", "\\\\").replace('"', '\\"')
        return f'handle:"{esc}"'
    return f"handle:{h}"


def _handle_exists(
    sess: requests.Session,
    handle: str,
    index: dict,
    *,
    live_check: bool,
) -> bool:
    if _index_has_handle(index, handle):
        return True
    if not live_check:
        return False
    q = _handle_search_query(handle)
    if not q:
        return False
    body = _gql(sess, _GQL_BY_HANDLE, {"q": q})
    nodes = (((body.get("data") or {}).get("products") or {}).get("nodes")) or []
    if nodes:
        index[normalize_shopify_product_handle(handle)] = {"id": nodes[0].get("id") or ""}
        return True
    return False


def _is_already_exists_error(err: str) -> bool:
    e = (err or "").upper()
    return "HANDLE_NOT_UNIQUE" in e or (
        "ALREADY EXISTS" in e and ("VARIANT" in e or "HANDLE" in e)
    )


def _create_product(
    sess: requests.Session,
    group: dict[str, Any],
    *,
    category_gid: str | None = None,
) -> tuple[str | None, str]:
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
        # Zelfde als CSV-export: Variant Inventory Tracker = shopify
        set_variants.append(
            {
                "optionValues": [{"optionName": opt_name, "name": v["option1_value"]}],
                "sku": v.get("sku") or "",
                "barcode": v.get("barcode") or "",
                "price": v.get("price") or "0.00",
                "inventoryPolicy": policy,
                "inventoryItem": {"tracked": True},
            }
        )

    inp: dict[str, Any] = {
        "title": group["title"],
        "handle": group["handle"],
        "descriptionHtml": group.get("body_html") or "",
        "vendor": group.get("vendor") or "",
        "productType": group.get("product_type") or "",
        "status": "ACTIVE" if group.get("published") else "DRAFT",
        "productOptions": [{"name": opt_name, "values": values or [{"name": "Default Title"}]}],
        "variants": set_variants,
    }
    if category_gid:
        inp["category"] = category_gid
    tags = group.get("tags") or []
    if tags:
        inp["tags"] = tags
    seo_title = group.get("seo_title") or ""
    seo_description = group.get("seo_description") or ""
    if seo_title or seo_description:
        seo: dict[str, str] = {}
        if seo_title:
            seo["title"] = seo_title
        if seo_description:
            seo["description"] = seo_description
        inp["seo"] = seo

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
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Bij cache-miss: GraphQL handle-check (default: aan)",
    )
    ap.add_argument("--refresh-cache", action="store_true")
    args = ap.parse_args()

    brands = [b.strip().lower() for b in args.brands.split(",") if b.strip()]
    if args.refresh_cache:
        os.environ["KTM_FORCE_REFRESH_SHOPIFY_CACHE"] = "1"

    print("Shopify products-index laden…", flush=True)
    index = get_shopify_products_index() or {}
    print(
        f"Handles in cache: {len(index)} (live-handle-check={'on' if args.live_handle_check else 'off'})",
        flush=True,
    )

    sess = _session()
    taxonomy_cache: dict[str, str | None] = {}
    publication_ids = _list_publication_ids(sess)
    if publication_ids:
        print(f"Publicatiekanalen: {len(publication_ids)} (alle kanalen)", flush=True)
    else:
        print(
            "Publicatiekanalen: 0 — app mist read_publications/write_publications; "
            "fallback Online Store only. Voeg die scopes toe voor alle kanalen.",
            flush=True,
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_delta_import_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    created = skipped = failed = 0
    done = 0
    by_brand: dict[str, dict[str, int]] = {
        b: {"created": 0, "skipped": 0, "failed": 0} for b in brands
    }

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "brand",
                "handle",
                "title",
                "result",
                "product_id",
                "error",
                "category",
                "channels",
            ],
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
                    by_brand[brand]["skipped"] += 1
                    w.writerow(
                        {
                            "brand": brand,
                            "handle": handle,
                            "title": g["title"],
                            "result": "skipped_exists",
                            "product_id": "",
                            "error": "",
                            "category": "",
                            "channels": "",
                        }
                    )
                    continue

                if args.dry_run:
                    created += 1
                    by_brand[brand]["created"] += 1
                    done += 1
                    w.writerow(
                        {
                            "brand": brand,
                            "handle": handle,
                            "title": g["title"],
                            "result": "dry_run_would_create",
                            "product_id": "",
                            "error": "",
                            "category": g.get("product_category") or "",
                            "channels": "all" if g.get("published") else "",
                        }
                    )
                    print(f"  [dry-run] {handle}", flush=True)
                    continue

                cat_path = g.get("product_category") or ""
                cat_gid = _resolve_taxonomy_gid(sess, cat_path, taxonomy_cache) if cat_path else None
                pid, err = _create_product(sess, g, category_gid=cat_gid)
                if not pid:
                    if _is_already_exists_error(err):
                        skipped += 1
                        by_brand[brand]["skipped"] += 1
                        index[normalize_shopify_product_handle(handle)] = {"id": ""}
                        w.writerow(
                            {
                                "brand": brand,
                                "handle": handle,
                                "title": g["title"],
                                "result": "skipped_exists_api",
                                "product_id": "",
                                "error": err,
                                "category": "",
                                "channels": "",
                            }
                        )
                        print(f"  SKIP exists {handle}", flush=True)
                    else:
                        failed += 1
                        by_brand[brand]["failed"] += 1
                        w.writerow(
                            {
                                "brand": brand,
                                "handle": handle,
                                "title": g["title"],
                                "result": "fail_create",
                                "product_id": "",
                                "error": err,
                                "category": "",
                                "channels": "",
                            }
                        )
                        print(f"  FAIL create {handle}: {err}", flush=True)
                    time.sleep(args.sleep)
                    continue

                extras: list[str] = []
                # productSet kan category negeren op oudere API → expliciete update
                if cat_gid:
                    cat_err = _set_category(sess, pid, cat_gid)
                    if cat_err:
                        extras.append(f"category:{cat_err}")
                elif cat_path:
                    extras.append("category:unresolved")

                ch_status = ""
                if g.get("published"):
                    ch_err = _publish_channels(sess, pid, publication_ids)
                    if ch_err:
                        extras.append(f"channels:{ch_err}")
                        ch_status = ch_err
                    elif publication_ids:
                        ch_status = f"ok:{len(publication_ids)}"
                    else:
                        ch_status = "ok:online_store"

                img_err = _attach_images(sess, pid, g["images"])
                if img_err:
                    extras.append(f"media:{img_err}")
                result = "ok" if not extras else "ok_with_warn"
                created += 1
                by_brand[brand]["created"] += 1
                done += 1
                index[normalize_shopify_product_handle(handle)] = {"id": pid}
                w.writerow(
                    {
                        "brand": brand,
                        "handle": handle,
                        "title": g["title"],
                        "result": result,
                        "product_id": pid,
                        "error": "; ".join(extras),
                        "category": cat_path if cat_gid else "",
                        "channels": ch_status,
                    }
                )
                print(
                    f"  OK {handle} id={pid} cat={'yes' if cat_gid else 'no'} "
                    f"ch={ch_status or '-'} media={img_err or 'ok'}",
                    flush=True,
                )
                time.sleep(args.sleep)

            print(
                f"{brand}: created={by_brand[brand]['created']} "
                f"skipped={by_brand[brand]['skipped']} "
                f"failed={by_brand[brand]['failed']}",
                flush=True,
            )

            if args.limit and done >= args.limit:
                break

    print(
        f"\nKlaar: created={created} skipped={skipped} failed={failed}\nRapport: {out}",
        flush=True,
    )
    for brand in brands:
        b = by_brand[brand]
        print(
            f"  {brand}: created={b['created']} skipped={b['skipped']} failed={b['failed']}",
            flush=True,
        )
    stats_path = ROOT / "output" / "logs" / "last_delta_import_stats.json"
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.write_text(
        json.dumps(
            {
                "at": datetime.now().isoformat(timespec="seconds"),
                "created": created if not args.dry_run else 0,
                "would_create": created if args.dry_run else created,
                "skipped": skipped,
                "failed": failed,
                "by_brand": by_brand,
                "dry_run": bool(args.dry_run),
                "report": str(out),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
