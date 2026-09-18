"""
Motox (ktm-shop-nederland) Shopify-caches onder cache/motox/.

Gescheiden van de KTM-shop caches in cache/, zodat merken/shops niet door elkaar lopen.

  from modules.motox_shopify_cache import refresh_motox_cache, load_motox_indexes

Caches:
  cache/motox/shopify_skus.json              — list[str] (uppercase SKUs)
  cache/motox/shopify_products_index.json    — handle -> {id, title, status}
  cache/motox/shopify_sku_to_product_id.json — sku -> product id
  cache/motox/shopify_handle_to_product_id.json — handle -> product id
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

try:
    import requests
except ImportError as e:  # pragma: no cover
    raise SystemExit("Installeer requests: pip install requests") from e

from modules.env_loader import load_dotenv
from modules.xml_loader import normalize_shopify_product_handle

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "cache" / "motox"
SKUS_FILE = CACHE_DIR / "shopify_skus.json"
PRODUCTS_INDEX_FILE = CACHE_DIR / "shopify_products_index.json"
SKU_TO_PRODUCT_ID_FILE = CACHE_DIR / "shopify_sku_to_product_id.json"
SKU_TO_VARIANT_ID_FILE = CACHE_DIR / "shopify_sku_to_variant_id.json"
HANDLE_TO_PRODUCT_ID_FILE = CACHE_DIR / "shopify_handle_to_product_id.json"
BULK_JSONL_FILE = CACHE_DIR / "shopify_products_bulk.jsonl"

_TIMEOUT = (12, 600)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        featuredImage {
          id
        }
        variants {
          edges {
            node {
              id
              sku
              price
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation MotoxEbihrCacheBulk {
  bulkOperationRunQuery(query: BULK_QUERY_PLACEHOLDER) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

_GQL_CURRENT = """
query {
  currentBulkOperation {
    id
    status
    errorCode
    objectCount
    url
  }
}
"""

_GQL_POLL = """
query MotoxCachePollBulk($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      status
      errorCode
      objectCount
      url
    }
  }
}
"""


def load_motox_env() -> tuple[str, str, str]:
    """
    Laadt .env.motox (override) en retourneert (domain, token, api_version).
    """
    load_dotenv(ROOT / ".env.motox", override=True)
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
        or "2024-10"
    ).strip()
    if not token or not domain:
        raise SystemExit(
            "Motox Shopify-creds ontbreken (SHOPIFY_ACCESS_TOKEN + "
            "SHOPIFY_SHOP_DOMAIN in .env.motox)."
        )
    return domain, token, api


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(
    sess: requests.Session,
    url: str,
    token: str,
    query: str,
    variables: dict | None = None,
) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    last: dict = {}
    last_exc: Exception | None = None
    for attempt in range(25):
        try:
            r = sess.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "X-Shopify-Access-Token": token,
                },
                json=payload,
                timeout=_TIMEOUT,
                proxies={"http": None, "https": None},
            )
            # Shopify gateway blips (502/503/504) — retry like throttle
            if r.status_code in (429, 502, 503, 504):
                wait = min(2.0 * (attempt + 1), 30.0)
                time.sleep(wait)
                continue
            r.raise_for_status()
            last = r.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            wait = min(2.0 * (attempt + 1), 30.0)
            time.sleep(wait)
            continue
        errs = last.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return last
    if last_exc is not None and not last:
        raise last_exc
    return last


def _gid_num(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def _wait_current_bulk(sess: requests.Session, gql_url: str, token: str) -> None:
    while True:
        body = _gql(sess, gql_url, token, _GQL_CURRENT)
        cur = ((body.get("data") or {}).get("currentBulkOperation")) or {}
        status = (cur.get("status") or "").upper()
        if not status or status in ("COMPLETED", "FAILED", "CANCELED"):
            return
        print(
            f"  Wacht op lopende Motox-bulk: {status} ({cur.get('objectCount')})",
            flush=True,
        )
        time.sleep(5)


def _run_bulk(sess: requests.Session, gql_url: str, token: str) -> Path:
    _wait_current_bulk(sess, gql_url, token)
    q = _GQL_BULK_START.replace(
        "BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY)
    )
    body = _gql(sess, gql_url, token, q)
    if body.get("errors"):
        raise SystemExit(f"GraphQL start-fout: {body.get('errors')}")
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    uerr = run.get("userErrors") or []
    if uerr:
        raise SystemExit(f"Bulk geweigerd: {uerr}")
    op_id = (run.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise SystemExit("Geen Motox bulk operation id.")
    print("Motox bulk-export gestart...", flush=True)
    poll_interval = 3.0
    while True:
        poll = _gql(sess, gql_url, token, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            url = node.get("url")
            if not url:
                raise SystemExit("Bulk klaar zonder URL.")
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with sess.get(
                url,
                stream=True,
                timeout=_TIMEOUT,
                proxies={"http": None, "https": None},
            ) as r:
                r.raise_for_status()
                with BULK_JSONL_FILE.open("wb") as f:
                    for chunk in r.iter_content(1024 * 256):
                        if chunk:
                            f.write(chunk)
            return BULK_JSONL_FILE
        if status in ("FAILED", "CANCELED"):
            raise SystemExit(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)


def _parse_bulk(path: Path) -> dict[str, dict]:
    """product_gid -> {id, handle, title, status, skus}"""
    products: dict[str, dict] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            gid = obj.get("id") or ""
            parent = obj.get("__parentId")
            if gid.startswith("gid://shopify/Product/") and not parent:
                feat = obj.get("featuredImage")
                products[gid] = {
                    "id": _gid_num(gid),
                    "handle": normalize_shopify_product_handle(
                        obj.get("handle") or ""
                    ),
                    "title": (obj.get("title") or "").strip(),
                    "status": (obj.get("status") or "").strip().upper(),
                    "has_image": bool(feat and (feat.get("id") if isinstance(feat, dict) else feat)),
                    "skus": [],
                }
                continue
            # Bulk JSONL variant rows often have only sku + __parentId (no id).
            is_variant = bool(parent) and (
                gid.startswith("gid://shopify/ProductVariant/")
                or ("sku" in obj and not gid.startswith("gid://shopify/Product/"))
            )
            if is_variant and parent in products:
                sku = (obj.get("sku") or "").strip()
                if sku:
                    products[parent]["skus"].append(
                        {
                            "sku": sku,
                            "variant_id": _gid_num(gid) if gid else "",
                            "price": (obj.get("price") or "").strip(),
                        }
                    )
    return products


def _write_caches_from_products(products: dict[str, dict]) -> dict:
    skus: set[str] = set()
    products_index: dict[str, dict] = {}
    sku_to_pid: dict[str, str] = {}
    sku_to_vid: dict[str, str] = {}
    handle_to_pid: dict[str, str] = {}

    for p in products.values():
        handle = (p.get("handle") or "").strip()
        pid = (p.get("id") or "").strip()
        if handle and pid:
            products_index[handle] = {
                "id": pid,
                "title": p.get("title") or "",
                "status": p.get("status") or "",
                "has_image": bool(p.get("has_image")),
            }
            handle_to_pid[handle] = pid
        for entry in p.get("skus") or []:
            # back-compat: plain string or dict
            if isinstance(entry, str):
                s = entry.strip()
                vid = ""
            else:
                s = (entry.get("sku") or "").strip()
                vid = (entry.get("variant_id") or "").strip()
            if not s:
                continue
            skus.add(s.upper())
            if pid and s not in sku_to_pid:
                sku_to_pid[s] = pid
            if pid and s.upper() not in sku_to_pid:
                sku_to_pid[s.upper()] = pid
            if vid:
                if s not in sku_to_vid:
                    sku_to_vid[s] = vid
                if s.upper() not in sku_to_vid:
                    sku_to_vid[s.upper()] = vid

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SKUS_FILE.write_text(
        json.dumps(sorted(skus), ensure_ascii=False), encoding="utf-8"
    )
    PRODUCTS_INDEX_FILE.write_text(
        json.dumps(products_index, ensure_ascii=False), encoding="utf-8"
    )
    SKU_TO_PRODUCT_ID_FILE.write_text(
        json.dumps(sku_to_pid, ensure_ascii=False), encoding="utf-8"
    )
    SKU_TO_VARIANT_ID_FILE.write_text(
        json.dumps(sku_to_vid, ensure_ascii=False), encoding="utf-8"
    )
    HANDLE_TO_PRODUCT_ID_FILE.write_text(
        json.dumps(handle_to_pid, ensure_ascii=False), encoding="utf-8"
    )

    summary = {
        "products": len(products_index),
        "skus": len(skus),
        "sku_to_product_id": len(sku_to_pid),
        "sku_to_variant_id": len(sku_to_vid),
        "handles": len(handle_to_pid),
        "cache_dir": str(CACHE_DIR),
    }
    print(
        f"Motox-cache geschreven: {summary['products']} producten, "
        f"{summary['skus']} SKUs, {summary['sku_to_variant_id']} variant-ids → {CACHE_DIR}",
        flush=True,
    )
    return summary


def refresh_motox_cache(*, force: bool = True) -> dict:
    """
    Haalt de volledige Motox-catalogus op via GraphQL bulk en schrijft cache/motox/.
    """
    if (
        not force
        and SKUS_FILE.is_file()
        and PRODUCTS_INDEX_FILE.is_file()
        and SKU_TO_PRODUCT_ID_FILE.is_file()
    ):
        print("Motox-cache bestaat al; gebruik force=True om te verversen.", flush=True)
        return load_motox_indexes()["summary"]

    domain, token, api = load_motox_env()
    gql_url = f"https://{domain}/admin/api/{api}/graphql.json"
    sess = _session()
    print(f"Motox-cache verversen voor {domain}...", flush=True)
    jsonl = _run_bulk(sess, gql_url, token)
    products = _parse_bulk(jsonl)
    return _write_caches_from_products(products)


def _load_json(path: Path, default):
    if not path.is_file():
        return default
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def load_motox_indexes() -> dict:
    """
    Laadt bestaande Motox-caches (geen netwerk).

    Returns dict with:
      skus: set[str] (uppercase)
      handles: set[str]
      products_index: dict
      sku_to_product_id: dict[str, str]
      sku_to_variant_id: dict[str, str]
      handle_to_product_id: dict[str, str]
      summary: dict
    """
    skus_list = _load_json(SKUS_FILE, [])
    products_index = _load_json(PRODUCTS_INDEX_FILE, {})
    sku_to_pid = _load_json(SKU_TO_PRODUCT_ID_FILE, {})
    sku_to_vid = _load_json(SKU_TO_VARIANT_ID_FILE, {})
    handle_to_pid = _load_json(HANDLE_TO_PRODUCT_ID_FILE, {})
    if not handle_to_pid and products_index:
        handle_to_pid = {
            h: (v.get("id") or "")
            for h, v in products_index.items()
            if (v.get("id") or "")
        }
    skus = {str(s).strip().upper() for s in skus_list if str(s).strip()}
    handles = {
        normalize_shopify_product_handle(h)
        for h in products_index.keys()
        if normalize_shopify_product_handle(h)
    }
    summary = {
        "products": len(products_index),
        "skus": len(skus),
        "sku_to_product_id": len(sku_to_pid),
        "sku_to_variant_id": len(sku_to_vid),
        "handles": len(handles),
        "cache_dir": str(CACHE_DIR),
        "present": SKUS_FILE.is_file() and PRODUCTS_INDEX_FILE.is_file(),
    }
    return {
        "skus": skus,
        "handles": handles,
        "products_index": products_index,
        "sku_to_product_id": sku_to_pid,
        "sku_to_variant_id": sku_to_vid,
        "handle_to_product_id": handle_to_pid,
        "summary": summary,
    }
