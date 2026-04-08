#!/usr/bin/env python3
"""
Haalt alle variant-SKU's uit Shopify op en schrijft SKU → lijst van { variant_id, product_id }
naar JSON (alle varianten met dezelfde SKU; duplicaat-SKU's worden allemaal opgenomen).

Los aan te roepen (handmatig, of later op een server op vaste tijden). Scripts zoals
`shopify_sync_eta_from_0150.py` en `shopify_sync_from_0150.py` gebruiken deze cache;
ze halen zelf geen volledige variantlijst meer op. Oude cache (alleen variant-id string)
wordt nog ondersteund bij inlezen.

Vereist: SHOPIFY_ACCESS_TOKEN in .env (zie .env.example).

Uitvoer default: cache/shopify_eta_sync_sku_variant.json
"""

from __future__ import annotations

import argparse
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CACHE = PROJECT_ROOT / "cache" / "shopify_eta_sync_sku_variant.json"


def load_dotenv(path: Path | None = None) -> None:
    path = path or (PROJECT_ROOT / ".env")
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


def fetch_sku_to_variant_cache(
    shop: str,
    token: str,
    api_version: str,
) -> dict[str, list[dict[str, str]]]:
    """SKU (uppercase) -> lijst van { variant_id, product_id } (alle varianten met die SKU)."""
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    out: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_ids: dict[str, set[str]] = defaultdict(set)
    url = f"https://{shop}/admin/api/{api_version}/variants.json?limit=250&fields=id,sku,product_id"
    timeout = (12, 120)
    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=timeout,
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
        n_var = sum(len(x) for x in out.values())
        print(f"  Varianten geladen: {len(out)} SKU's, {n_var} varianten…", flush=True)
        url = _next_page_url(r.headers.get("Link"))
        time.sleep(0.5)
    return dict(out)


def main() -> int:
    load_dotenv()

    p = argparse.ArgumentParser(
        description="Shopify: SKU→variant_id+product_id cache bouwen (sync-scripts)."
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_CACHE,
        help=f"JSON-uitvoer (default: {DEFAULT_CACHE})",
    )
    args = p.parse_args()

    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
    api_ver = os.environ.get("SHOPIFY_ADMIN_API_VERSION", "2024-10").strip()

    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env of export).", flush=True)
        return 1

    out_path = args.output.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("Shopify: alle varianten ophalen (SKU → variant_id + product_id)...", flush=True)
    sku_cache = fetch_sku_to_variant_cache(shop, token, api_ver)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sku_cache, f, ensure_ascii=False, indent=2)
    n_v = sum(len(v) for v in sku_cache.values())
    n_multi = sum(1 for v in sku_cache.values() if len(v) > 1)
    print(
        f"Klaar: {len(sku_cache)} SKU's ({n_v} varianten) weggeschreven naar {out_path}",
        flush=True,
    )
    if n_multi:
        print(
            f"  ({n_multi} SKU's komen meer dan eens voor in Shopify — alle varianten staan in de cache.)",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
