from __future__ import annotations

import json
import os
import time

import requests

"""
Shopify REST-caches onder cache/:

- KTM_SHOPIFY_CACHE_MAX_AGE_DAYS — als gezet (bijv. 7), worden bestanden ouder dan zoveel
  dagen bij de eerstvolgende run automatisch opnieuw opgehaald (geen handmatig wissen).
- KTM_FORCE_REFRESH_SHOPIFY_CACHE=1 — altijd opnieuw ophalen en cache overschrijven.
- KTM_SKIP_SHOPIFY_API=1 — nooit netwerk; alleen bestaande cache (handig offline).

Zie docs/shopify_cache_en_scheduling.md voor periodiek draaien (launchd/cron).
"""

CACHE_FILE = "cache/shopify_skus.json"
PRODUCTS_CACHE_FILE = "cache/shopify_products_index.json"
# Variant SKU (shop) -> parent product id (for YMM / fitment rows)
SKU_TO_PRODUCT_ID_CACHE = "cache/shopify_sku_to_product_id.json"

SHOP = "ktm-shop-nl.myshopify.com"
TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip() or (
    "REDACTED_REVOKE_AND_ROTATE"
)

# Connect timeout, read timeout (seconds) — fail faster than hanging on bad proxy/VPN
_REQUEST_TIMEOUT = (12, 120)

# Ignore HTTP(S)_PROXY and macOS proxy auto-config (often causes tunnel 403)
_session: requests.Session | None = None


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def _shopify_cache_max_age_days() -> float | None:
    """
    None = geen TTL: cache blijft geldig tot handmatig verwijderen of --refresh.
    """
    raw = os.environ.get("KTM_SHOPIFY_CACHE_MAX_AGE_DAYS", "").strip()
    if not raw:
        return None
    try:
        d = float(raw)
    except ValueError:
        return None
    return d if d > 0 else None


def _cache_file_stale(path: str, max_age_days: float) -> bool:
    if not os.path.exists(path):
        return True
    age_sec = time.time() - os.path.getmtime(path)
    return age_sec > max_age_days * 86400


def _http_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.trust_env = False
    return _session


def get_all_shopify_skus():

    if os.path.exists(CACHE_FILE):
        print("Shopify SKU cache laden...")
        with open(CACHE_FILE, "r") as f:
            return set(json.load(f))

    print("Shopify API scannen...")

    skus = set()
    url = f"https://{SHOP}/admin/api/2024-01/variants.json?limit=250"

    headers = {
        "X-Shopify-Access-Token": TOKEN
    }
    sess = _http_session()

    while url:

        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )

        if r.status_code == 429:
            print("Rate limit, wachten...")
            time.sleep(2)
            continue

        if r.status_code >= 500:
            print("Shopify server fout, retry...")
            time.sleep(3)
            continue

        try:
            data = r.json()
        except Exception:
            print("JSON parse fout, retry...")
            time.sleep(2)
            continue

        for v in data["variants"]:
            if v["sku"]:
                skus.add(v["sku"])

        print("SKU count:", len(skus))

        link = r.headers.get("Link")

        next_url = None

        if link:
            parts = link.split(",")
            for p in parts:
                if 'rel="next"' in p:
                    next_url = p.split(";")[0].strip().replace("<", "").replace(">", "")

        url = next_url

        time.sleep(0.5)

    print("Shopify SKUs totaal:", len(skus))

    with open(CACHE_FILE, "w") as f:
        json.dump(list(skus), f)

    return skus


def get_shopify_products_index(force_refresh: bool = False):
    """
    Return Shopify product index keyed by handle:
    {
      "<handle>": {"id": "...", "created_at": "...", "title": "...", "tags": "..."}
    }
    """
    skip = os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if skip:
        if os.path.exists(PRODUCTS_CACHE_FILE):
            print(
                "KTM_SKIP_SHOPIFY_API=1: Shopify API overgeslagen, cache gebruiken...",
                flush=True,
            )
            with open(PRODUCTS_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        print(
            "KTM_SKIP_SHOPIFY_API=1: geen cache; alleen input/Product-Ids CSV als fallback.",
            flush=True,
        )
        return {}

    max_age = _shopify_cache_max_age_days()
    force_refresh = force_refresh or _env_truthy("KTM_FORCE_REFRESH_SHOPIFY_CACHE")

    if not force_refresh and os.path.exists(PRODUCTS_CACHE_FILE):
        if max_age is not None and _cache_file_stale(PRODUCTS_CACHE_FILE, max_age):
            print(
                f"Shopify productindex-cache ouder dan {max_age} dagen — opnieuw ophalen...",
                flush=True,
            )
        else:
            print("Shopify productindex cache laden...", flush=True)
            with open(PRODUCTS_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)

    if force_refresh and os.path.exists(PRODUCTS_CACHE_FILE):
        print("Shopify productindex-cache geforceerd verversen...", flush=True)

    print("Shopify products ophalen (direct, zonder systeem-proxy)...", flush=True)
    index = {}
    url = (
        f"https://{SHOP}/admin/api/2024-01/products.json"
        "?limit=250&fields=id,created_at,handle,title,tags"
    )
    headers = {"X-Shopify-Access-Token": TOKEN}
    sess = _http_session()

    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )

        if r.status_code == 429:
            print("Rate limit products, wachten...")
            time.sleep(2)
            continue

        if r.status_code >= 500:
            print("Shopify server fout products, retry...")
            time.sleep(3)
            continue

        r.raise_for_status()
        data = r.json()

        for p in data.get("products", []):
            handle = (p.get("handle") or "").strip()
            if not handle:
                continue
            index[handle] = {
                "id": str(p.get("id") or ""),
                "created_at": p.get("created_at") or "",
                "title": p.get("title") or "",
                "tags": p.get("tags") or "",
            }

        print("Product count:", len(index))

        link = r.headers.get("Link")
        next_url = None
        if link:
            parts = link.split(",")
            for p in parts:
                if 'rel="next"' in p:
                    next_url = p.split(";")[0].strip().replace("<", "").replace(">", "")
        url = next_url
        time.sleep(0.5)

    os.makedirs(os.path.dirname(PRODUCTS_CACHE_FILE), exist_ok=True)
    with open(PRODUCTS_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f)

    return index


def _shopify_skip_api() -> bool:
    return os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def get_shopify_sku_to_product_id(force_refresh: bool = False) -> dict[str, str]:
    """
    Map variant SKU (as stored in Shopify) -> Shopify product id (numeric string).
    Used for YMM fitment: XML uses article SKUs; product handle in Shopify often differs.
    """
    if _shopify_skip_api():
        if os.path.exists(SKU_TO_PRODUCT_ID_CACHE):
            print(
                "KTM_SKIP_SHOPIFY_API=1: SKU→Product Id cache laden...",
                flush=True,
            )
            with open(SKU_TO_PRODUCT_ID_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    max_age = _shopify_cache_max_age_days()
    force_refresh = force_refresh or _env_truthy("KTM_FORCE_REFRESH_SHOPIFY_CACHE")

    if not force_refresh and os.path.exists(SKU_TO_PRODUCT_ID_CACHE):
        if max_age is not None and _cache_file_stale(SKU_TO_PRODUCT_ID_CACHE, max_age):
            print(
                f"Shopify SKU→Product Id-cache ouder dan {max_age} dagen — opnieuw ophalen...",
                flush=True,
            )
        else:
            print("Shopify SKU→Product Id cache laden...", flush=True)
            with open(SKU_TO_PRODUCT_ID_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)

    if force_refresh and os.path.exists(SKU_TO_PRODUCT_ID_CACHE):
        print("Shopify SKU→Product Id-cache geforceerd verversen...", flush=True)

    print(
        "Shopify varianten ophalen voor SKU→Product Id (kan even duren)...",
        flush=True,
    )
    out: dict[str, str] = {}
    url = (
        f"https://{SHOP}/admin/api/2024-01/variants.json"
        "?limit=250&fields=sku,product_id"
    )
    headers = {"X-Shopify-Access-Token": TOKEN}
    sess = _http_session()

    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )

        if r.status_code == 429:
            print("Rate limit variants, wachten...", flush=True)
            time.sleep(2)
            continue

        if r.status_code >= 500:
            print("Shopify server fout variants, retry...", flush=True)
            time.sleep(3)
            continue

        r.raise_for_status()
        data = r.json()

        for v in data.get("variants", []):
            sku = (v.get("sku") or "").strip()
            pid = v.get("product_id")
            if not sku or pid is None or sku in out:
                continue
            out[sku] = str(int(pid)) if isinstance(pid, (int, float)) else str(pid)

        print("SKU→Product Id entries:", len(out), flush=True)

        link = r.headers.get("Link")
        next_url = None
        if link:
            parts = link.split(",")
            for p in parts:
                if 'rel="next"' in p:
                    next_url = p.split(";")[0].strip().replace("<", "").replace(">", "")
        url = next_url
        time.sleep(0.5)

    os.makedirs(os.path.dirname(SKU_TO_PRODUCT_ID_CACHE) or ".", exist_ok=True)
    with open(SKU_TO_PRODUCT_ID_CACHE, "w", encoding="utf-8") as f:
        json.dump(out, f)

    return out
