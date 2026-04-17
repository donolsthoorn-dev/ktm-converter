#!/usr/bin/env python3
"""
Vindt variant-SKU's die op meer dan één product voorkomen binnen dezelfde vendor.

Zelfde SKU bij verschillende vendors (bijv. KTM vs. HUSQVARNA) telt niet als
duplicaat. Schrijft een CSV met per gedeelde SKU (per vendor) één rij per
betrokken product: vendor, handle, variant-SKU('s), storefront- en admin-URL,
Shopify-status (active/draft/archived).

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN in .env (zie .env.example).

Schaduw-cache (default ``cache/shopify_duplicate_sku_shadow.json``): tweede run is lokaal
en snel. Eerste run (of ``--refresh``) haalt varianten + productmetadata op bij Shopify.

  python3 scripts/shopify_list_duplicate_variant_skus.py
  python3 scripts/shopify_list_duplicate_variant_skus.py --refresh
  python3 scripts/shopify_list_duplicate_variant_skus.py --max-age-hours 24
  python3 scripts/shopify_list_duplicate_variant_skus.py -o output/duplicate_variant_skus.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
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

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
API_VER = config.SHOPIFY_ADMIN_API_VERSION
SHOP_SLUG = config.SHOPIFY_SHOP_SLUG
_REQUEST_TIMEOUT = (12, 120)
_DEFAULT_OUT = ROOT / "output" / "duplicate_variant_skus.csv"
_DEFAULT_SHADOW = ROOT / "cache" / "shopify_duplicate_sku_shadow.json"
_SHADOW_SCHEMA_VERSION = 2


def load_dotenv(path: Path | None = None) -> None:
    path = path or (ROOT / ".env")
    if not path.is_file():
        return
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                if not key:
                    continue
                val = val.strip()
                if (val.startswith('"') and val.endswith('"')) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                if key not in os.environ:
                    os.environ[key] = val
    except OSError:
        return


def _http_session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def _next_page_url(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip()
            return url.replace("<", "").replace(">", "")
    return None


def fetch_sku_to_variant_entries(
    shop: str,
    token: str,
    api_version: str,
) -> dict[str, list[dict[str, str]]]:
    """SKU (uppercase) -> lijst van { variant_id, product_id }."""
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    out: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_ids: dict[str, set[str]] = defaultdict(set)
    url = (
        f"https://{shop}/admin/api/{api_version}/variants.json"
        "?limit=250&fields=id,sku,product_id"
    )
    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("REST rate limit (variants), wachten...", flush=True)
            time.sleep(2)
            continue
        if r.status_code >= 500:
            print("Shopify serverfout (variants), retry...", flush=True)
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()
        for v in data.get("variants", []):
            sku = (v.get("sku") or "").strip().upper()
            vid = v.get("id")
            pid = v.get("product_id")
            if not sku or vid is None:
                continue
            vid_s = str(int(vid)) if isinstance(vid, (int, float)) else str(vid)
            if vid_s in seen_ids[sku]:
                continue
            seen_ids[sku].add(vid_s)
            if pid is None:
                pid_s = ""
            else:
                pid_s = str(int(pid)) if isinstance(pid, (int, float)) else str(pid).strip()
            out[sku].append({"variant_id": vid_s, "product_id": pid_s})
        print(f"  Varianten… {sum(len(x) for x in out.values())} regels", flush=True)
        url = _next_page_url(r.headers.get("Link"))
        time.sleep(0.5)
    return dict(out)


def fetch_product_id_to_handle_status_vendor(
    shop: str,
    token: str,
    api_version: str,
) -> dict[str, dict[str, str]]:
    """product_id -> { handle, status, vendor, type }."""
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    out: dict[str, dict[str, str]] = {}
    url = (
        f"https://{shop}/admin/api/{api_version}/products.json"
        "?limit=250&fields=id,handle,status,vendor,product_type"
    )
    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("REST rate limit (products), wachten...", flush=True)
            time.sleep(2)
            continue
        if r.status_code >= 500:
            print("Shopify serverfout (products), retry...", flush=True)
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()
        for p in data.get("products", []):
            pid = p.get("id")
            if pid is None:
                continue
            pid_s = str(int(pid)) if isinstance(pid, (int, float)) else str(pid)
            handle = (p.get("handle") or "").strip()
            status = (p.get("status") or "").strip()
            vendor = (p.get("vendor") or "").strip()
            ptype = (p.get("product_type") or "").strip()
            out[pid_s] = {
                "handle": handle,
                "status": status,
                "vendor": vendor,
                "type": ptype,
            }
        print(f"  Producten… {len(out)}", flush=True)
        url = _next_page_url(r.headers.get("Link"))
        time.sleep(0.5)
    return out


def storefront_url(handle: str) -> str:
    h = handle.strip()
    return f"https://{SHOP_SLUG}.myshopify.com/products/{h}"


def admin_product_url(product_id: str) -> str:
    return f"https://admin.shopify.com/store/{SHOP_SLUG}/products/{product_id}"


def _shadow_cache_valid(
    meta: dict,
    shop: str,
    api_version: str,
    max_age_hours: float | None,
) -> bool:
    if meta.get("shop_domain") != shop or meta.get("api_version") != api_version:
        return False
    if int(meta.get("schema_version") or 1) != _SHADOW_SCHEMA_VERSION:
        return False
    if max_age_hours is None:
        return True
    fetched = meta.get("fetched_at")
    if not isinstance(fetched, (int, float)):
        return False
    return (time.time() - float(fetched)) <= max_age_hours * 3600.0


def load_shadow_state(
    path: Path,
    shop: str,
    api_version: str,
    max_age_hours: float | None,
) -> tuple[
    dict[str, list[dict[str, str]]], dict[str, dict[str, str]], float | None
] | None:
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    meta = raw.get("meta") or {}
    if not _shadow_cache_valid(meta, shop, api_version, max_age_hours):
        return None
    fetched_at: float | None = None
    ft = meta.get("fetched_at")
    if isinstance(ft, (int, float)):
        fetched_at = float(ft)
    sku_entries = raw.get("sku_entries")
    products = raw.get("products")
    if not isinstance(sku_entries, dict) or not isinstance(products, dict):
        return None
    out_sku: dict[str, list[dict[str, str]]] = {}
    for sku, entries in sku_entries.items():
        if not isinstance(sku, str) or not isinstance(entries, list):
            continue
        norm: list[dict[str, str]] = []
        for e in entries:
            if isinstance(e, dict):
                norm.append(
                    {
                        "variant_id": str(e.get("variant_id", "")),
                        "product_id": str(e.get("product_id", "")),
                    }
                )
        out_sku[sku] = norm
    out_p: dict[str, dict[str, str]] = {}
    for pid, info in products.items():
        if not isinstance(pid, str) or not isinstance(info, dict):
            continue
        out_p[pid] = {
            "handle": str(info.get("handle", "")),
            "status": str(info.get("status", "")),
            "vendor": str(info.get("vendor", "")),
            "type": str(info.get("type", "")),
        }
    return out_sku, out_p, fetched_at


def save_shadow_state(
    path: Path,
    shop: str,
    api_version: str,
    sku_entries: dict[str, list[dict[str, str]]],
    products: dict[str, dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "shop_domain": shop,
            "api_version": api_version,
            "schema_version": _SHADOW_SCHEMA_VERSION,
            "fetched_at": time.time(),
        },
        "sku_entries": dict(sku_entries),
        "products": dict(products),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp.replace(path)


def main() -> int:
    load_dotenv()
    p = argparse.ArgumentParser(
        description=(
            "CSV: dezelfde variant-SKU op meerdere producten, alleen binnen dezelfde vendor."
        )
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_DEFAULT_OUT,
        help=f"Uitvoer-CSV (default: {_DEFAULT_OUT})",
    )
    p.add_argument(
        "--cache",
        type=Path,
        default=_DEFAULT_SHADOW,
        help=f"Schaduw-cache JSON (default: {_DEFAULT_SHADOW})",
    )
    p.add_argument(
        "--refresh",
        action="store_true",
        help="Negeer cache; haal alles opnieuw op bij Shopify en overschrijf de cache.",
    )
    p.add_argument(
        "--max-age-hours",
        type=float,
        default=None,
        metavar="N",
        help=(
            "Cache maximaal N uur geldig; daarna opnieuw ophalen. "
            "Zonder deze optie blijft een geldige cache onbeperkt bruikbaar tot --refresh."
        ),
    )
    args = p.parse_args()

    if not TOKEN.strip():
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", flush=True)
        return 1

    cache_path = args.cache.resolve()
    loaded = None
    if not args.refresh:
        loaded = load_shadow_state(cache_path, SHOP, API_VER, args.max_age_hours)

    if loaded is not None:
        sku_entries, prod_map, fetched_at = loaded
        age_part = ""
        if fetched_at is not None:
            age_part = f", {int((time.time() - fetched_at) // 60)} min geleden bijgewerkt"
        print(
            f"Schaduw-cache gebruikt: {cache_path} "
            f"({len(sku_entries)} SKU-sleutels, {len(prod_map)} producten{age_part}).",
            flush=True,
        )
    else:
        if args.refresh:
            print("Shopify: --refresh — volledige fetch…", flush=True)
        elif cache_path.is_file():
            print(
                "Schaduw-cache ongeldig of teoud — opnieuw ophalen bij Shopify…",
                flush=True,
            )
        else:
            print(
                "Geen schaduw-cache — eerste keer ophalen bij Shopify (daarna snel lokaal)…",
                flush=True,
            )
        print("Shopify: alle varianten ophalen…", flush=True)
        sku_entries = fetch_sku_to_variant_entries(SHOP, TOKEN, API_VER)

        print("Shopify: product-metadata (handle, status, vendor) ophalen…", flush=True)
        prod_map = fetch_product_id_to_handle_status_vendor(SHOP, TOKEN, API_VER)

        save_shadow_state(cache_path, SHOP, API_VER, sku_entries, prod_map)
        print(f"Schaduw-cache opgeslagen: {cache_path}", flush=True)

    duplicate_rows: list[dict[str, str]] = []
    for sku, entries in sorted(sku_entries.items()):
        by_vendor: dict[str, list[dict[str, str]]] = defaultdict(list)
        for e in entries:
            pid = e.get("product_id") or ""
            if not pid:
                continue
            vendor = (prod_map.get(pid) or {}).get("vendor", "").strip()
            by_vendor[vendor].append(e)

        for vendor, ventries in sorted(by_vendor.items(), key=lambda x: x[0].casefold()):
            product_ids = {e["product_id"] for e in ventries if e.get("product_id")}
            if len(product_ids) < 2:
                continue
            by_product: dict[str, list[str]] = defaultdict(list)
            for e in ventries:
                pid = e.get("product_id") or ""
                if not pid:
                    continue
                by_product[pid].append(e.get("variant_id", ""))

            for pid in sorted(product_ids, key=lambda x: int(x) if x.isdigit() else x):
                variant_ids = by_product.get(pid, [])
                n = len(variant_ids)
                variant_skus_col = (
                    f"{sku} (×{n} varianten)" if n > 1 else sku
                )
                duplicate_rows.append(
                    {
                        "shared_sku": sku,
                        "vendor": vendor,
                        "product_id": pid,
                        "variant_ids": "; ".join(variant_ids),
                        "variant_skus": variant_skus_col,
                        "_sort_pid": pid,
                    }
                )

    if not duplicate_rows:
        print(
            "Geen SKU's gevonden die op 2+ verschillende producten voorkomen "
            "binnen dezelfde vendor.",
            flush=True,
        )
        out_path = args.output.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "shared_sku",
                    "vendor",
                    "product_id",
                    "handle",
                    "status",
                    "type",
                    "variant_skus",
                    "variant_ids",
                    "storefront_url",
                    "admin_url",
                ],
            )
            w.writeheader()
        print(f"Lege CSV geschreven: {out_path}", flush=True)
        return 0

    out_path = args.output.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "shared_sku",
        "vendor",
        "product_id",
        "handle",
        "status",
        "type",
        "variant_skus",
        "variant_ids",
        "storefront_url",
        "admin_url",
    ]

    duplicate_rows.sort(
        key=lambda r: (r["shared_sku"], r["vendor"].casefold(), r["_sort_pid"])
    )
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in duplicate_rows:
            pid = row["product_id"]
            info = prod_map.get(pid, {})
            handle = info.get("handle", "")
            status = info.get("status", "")
            ptype = info.get("type", "")
            row_out = {
                "shared_sku": row["shared_sku"],
                "vendor": row["vendor"],
                "product_id": pid,
                "handle": handle,
                "status": status,
                "type": ptype,
                "variant_skus": row["variant_skus"],
                "variant_ids": row["variant_ids"],
                "storefront_url": storefront_url(handle) if handle else "",
                "admin_url": admin_product_url(pid),
            }
            w.writerow(row_out)

    n_groups = len({(r["shared_sku"], r["vendor"]) for r in duplicate_rows})
    grp = "duplicaatgroep" if n_groups == 1 else "duplicaatgroepen"
    print(
        f"Klaar: {n_groups} {grp} (zelfde SKU binnen vendor), "
        f"{len(duplicate_rows)} rijen → {out_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
