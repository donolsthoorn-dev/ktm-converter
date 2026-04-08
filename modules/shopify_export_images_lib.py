"""
Gedeelde logica voor export-CSV vs live Shopify-afbeeldingen (compare + apply).
Gebruikt door scripts/shopify_compare_export_images.py en shopify_apply_missing_images.py.
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import requests

import config
from modules.xml_loader import normalize_shopify_product_handle

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
VERSION = config.SHOPIFY_ADMIN_API_VERSION
_REQUEST_TIMEOUT = (12, 120)

_GRAPHQL_URL = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"

_GQL_PRODUCTS_BY_SEARCH = """
query ProductsImageBatch($q: String!) {
  products(first: 250, query: $q) {
    edges {
      node {
        id
        handle
        images(first: 50) {
          edges {
            node {
              url
              originalSrc
            }
          }
        }
      }
    }
  }
}
"""

DEFAULT_TASKS_BASENAME = "shopify_missing_image_tasks.json"


def default_tasks_path() -> str:
    return os.path.join(config.LOG_OUTPUT_DIR, DEFAULT_TASKS_BASENAME)


def norm_src(url: str) -> str:
    if not url or not str(url).strip():
        return ""
    u = str(url).strip().split("?", 1)[0].rstrip("/")
    return u.lower()


def latest_all_csv(products_dir: str) -> str | None:
    paths = glob.glob(os.path.join(products_dir, "shopify_export_all_*.csv"))
    if not paths:
        return None
    return max(paths, key=os.path.getmtime)


def parse_csv_images(path: str) -> dict[str, list[str]]:
    """Handle (genormaliseerd) -> geordende lijst unieke Image Src-URL’s."""
    by_pos: dict[str, list[tuple[int, str]]] = defaultdict(list)
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}
        if "Handle" not in reader.fieldnames or "Image Src" not in reader.fieldnames:
            raise SystemExit(
                f"CSV mist verplichte kolommen Handle / Image Src: {path!r}"
            )
        for row in reader:
            h = normalize_shopify_product_handle(row.get("Handle") or "")
            if not h:
                continue
            src = (row.get("Image Src") or "").strip()
            if not src:
                continue
            raw_pos = (row.get("Image Position") or "").strip()
            try:
                ipos = int(raw_pos) if raw_pos else 9999
            except ValueError:
                ipos = 9999
            by_pos[h].append((ipos, src))

    out: dict[str, list[str]] = {}
    for h, pairs in by_pos.items():
        pairs.sort(key=lambda x: (x[0], x[1]))
        seen: set[str] = set()
        ordered: list[str] = []
        for _, url in pairs:
            n = norm_src(url)
            if not n or n in seen:
                continue
            seen.add(n)
            ordered.append(url.strip())
        if ordered:
            out[h] = ordered
    return out


def session_for_thread() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def get_product_images_by_handle(
    sess: requests.Session,
    handle: str,
) -> tuple[set[str], str]:
    url = f"https://{SHOP}/admin/api/{VERSION}/products.json"
    params = {"handle": handle, "fields": "id,handle,images"}
    headers = {"X-Shopify-Access-Token": TOKEN}

    for _attempt in range(10):
        r = sess.get(
            url,
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(2 + min(_attempt, 5) * 0.4)
            continue
        if r.status_code >= 500:
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()
        products = data.get("products") or []
        if not products:
            return set(), ""
        p = products[0]
        pid = p.get("id")
        pid_str = str(int(pid)) if isinstance(pid, (int, float)) else str(pid or "")
        norms: set[str] = set()
        for img in p.get("images") or []:
            src = img.get("src")
            if src:
                n = norm_src(src)
                if n:
                    norms.add(n)
        return norms, pid_str

    return set(), ""


def _gid_numeric(gid: str) -> str:
    if not gid:
        return ""
    return str(gid).rsplit("/", 1)[-1]


def _graphql_post(sess: requests.Session, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query, "variables": variables}
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
        r.raise_for_status()
        body = r.json()
        errs = body.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return body
    return body


_HANDLE_SIMPLE = re.compile(r"^[a-z0-9_-]+$", re.I)


def _handle_search_term(h: str) -> str:
    h = (h or "").strip()
    if not h:
        return ""
    if _HANDLE_SIMPLE.match(h):
        return f"handle:{h}"
    esc = h.replace("\\", "\\\\").replace('"', '\\"')
    return f'handle:"{esc}"'


def _build_products_search_query(batch_handles: list[str]) -> str:
    parts = [_handle_search_term(x) for x in batch_handles if _handle_search_term(x)]
    return " OR ".join(parts)


def _parse_graphql_product_node(node: dict[str, Any]) -> tuple[str, set[str], str]:
    handle = normalize_shopify_product_handle(node.get("handle") or "")
    pid = _gid_numeric(str(node.get("id") or ""))
    norms: set[str] = set()
    for e in ((node.get("images") or {}).get("edges")) or []:
        n = (e or {}).get("node") or {}
        url = n.get("url") or n.get("originalSrc")
        if url:
            ns = norm_src(str(url))
            if ns:
                norms.add(ns)
    return handle, norms, pid


def _graphql_products_batch(
    sess: requests.Session,
    query_string: str,
) -> dict[str, tuple[set[str], str]]:
    """Zoekquery → handle -> (norms, product_id)."""
    out: dict[str, tuple[set[str], str]] = {}
    if not query_string.strip():
        return out
    body = _graphql_post(sess, _GQL_PRODUCTS_BY_SEARCH, {"q": query_string})
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"], ensure_ascii=False)[:800])
    data = body.get("data") or {}
    conn = data.get("products")
    if not conn:
        return out
    for e in conn.get("edges") or []:
        node = (e or {}).get("node") or {}
        handle, norms, pid = _parse_graphql_product_node(node)
        if handle and pid:
            out[handle] = (norms, pid)
    return out


def fetch_handle_maps_rest_only(
    handles: list[str],
    workers: int,
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """Eén REST GET per handle (langzaam bij grote sets)."""
    norms_by_handle: dict[str, set[str]] = {}
    id_by_handle: dict[str, str] = {}
    total = len(handles)
    if total == 0:
        return norms_by_handle, id_by_handle

    lock = threading.Lock()
    done = 0

    def run_one(h: str) -> tuple[str, set[str], str]:
        nonlocal done
        sess = session_for_thread()
        norms, pid = get_product_images_by_handle(sess, h)
        with lock:
            done += 1
            if done % 250 == 0 or done == total:
                print(f"  Live opgehaald (REST): {done}/{total} handles...", flush=True)
        return h, norms, pid

    w = max(1, min(workers, total))
    with ThreadPoolExecutor(max_workers=w) as pool:
        futures = [pool.submit(run_one, h) for h in handles]
        for fut in as_completed(futures):
            h, norms, pid = fut.result()
            if pid:
                id_by_handle[h] = pid
                norms_by_handle[h] = norms

    return norms_by_handle, id_by_handle


def fetch_handle_maps_for_handles(
    handles: list[str],
    rest_workers: int = 8,
    *,
    graphql_batch: int = 25,
    fetch_workers: int = 12,
    rest_only: bool = False,
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """
    Haal live image-src’s + product-id’s op voor gegeven handles.

    Standaard: GraphQL `products(query: "handle:a OR handle:b …")` in batches
    (veel minder API-rondes dan REST per handle), daarna REST voor ontbrekende handles.
    """
    norms_by_handle: dict[str, set[str]] = {}
    id_by_handle: dict[str, str] = {}
    total = len(handles)
    if total == 0:
        return norms_by_handle, id_by_handle

    if rest_only:
        return fetch_handle_maps_rest_only(handles, rest_workers)

    uniq = list(dict.fromkeys(handles))
    bs = max(3, min(graphql_batch, 50))
    batches: list[list[str]] = [uniq[i : i + bs] for i in range(0, len(uniq), bs)]

    print(
        f"  Ophalen via GraphQL: {len(uniq)} handles in {len(batches)} batch(es) "
        f"(±{bs} handles/query, parallel {fetch_workers})...",
        flush=True,
    )

    lock = threading.Lock()
    done_batches = 0
    merged: dict[str, tuple[set[str], str]] = {}

    def run_batch(batch: list[str]) -> None:
        nonlocal done_batches
        q = _build_products_search_query(batch)
        sess = session_for_thread()
        try:
            part = _graphql_products_batch(sess, q)
        except Exception as e:
            with lock:
                done_batches += 1
                print(
                    f"  GraphQL-batch fout ({done_batches}/{len(batches)}): {e}",
                    flush=True,
                )
            return
        with lock:
            merged.update(part)
            done_batches += 1
            if done_batches % 5 == 0 or done_batches == len(batches):
                print(
                    f"  GraphQL: {done_batches}/{len(batches)} batches, "
                    f"{len(merged)} producten bekend...",
                    flush=True,
                )

    fw = max(1, min(fetch_workers, len(batches)))
    with ThreadPoolExecutor(max_workers=fw) as pool:
        list(pool.map(run_batch, batches))

    for h, (norms, pid) in merged.items():
        if pid:
            id_by_handle[h] = pid
            norms_by_handle[h] = norms

    missing = [h for h in uniq if h not in id_by_handle]
    if missing:
        print(
            f"  REST fallback voor {len(missing)} handle(s) (niet in GraphQL-resultaat)…",
            flush=True,
        )
        n_rest, id_rest = fetch_handle_maps_rest_only(missing, rest_workers)
        norms_by_handle.update(n_rest)
        id_by_handle.update(id_rest)

    print(f"  Live opgehaald: {len(id_by_handle)}/{total} handles met product-id.", flush=True)
    return norms_by_handle, id_by_handle


def post_product_image(
    sess: requests.Session,
    product_id: str,
    src: str,
) -> tuple[bool, str]:
    url = f"https://{SHOP}/admin/api/{VERSION}/products/{product_id}/images.json"
    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }
    body = {"image": {"src": src}}
    r = sess.post(
        url,
        headers=headers,
        json=body,
        timeout=_REQUEST_TIMEOUT,
        proxies={"http": None, "https": None},
    )
    if r.status_code == 429:
        return False, "429"
    if r.status_code >= 500:
        return False, f"HTTP {r.status_code}"
    if r.status_code not in (200, 201):
        try:
            err = r.json()
        except Exception:
            err = r.text[:500]
        return False, str(err)
    return True, ""


def post_product_image_retries(
    sess: requests.Session,
    product_id: str,
    src: str,
) -> tuple[bool, str]:
    for attempt in range(12):
        ok, err = post_product_image(sess, product_id, src)
        if ok:
            return True, ""
        if err == "429":
            time.sleep(2 + min(attempt, 8) * 0.35)
            continue
        if err.startswith("HTTP 5"):
            time.sleep(2 + attempt * 0.25)
            continue
        return False, err
    return False, "te veel retries"


def apply_missing_images_parallel(
    tasks: list[tuple[str, str, str]],
    workers: int,
) -> tuple[int, int]:
    total = len(tasks)
    if total == 0:
        return 0, 0

    lock = threading.Lock()
    done = 0
    ok_c = 0
    fail_c = 0

    def run_one(item: tuple[str, str, str]) -> tuple[bool, str]:
        nonlocal done, ok_c, fail_c
        handle, pid, src = item
        sess = session_for_thread()
        success, err = post_product_image_retries(sess, pid, src)
        with lock:
            done += 1
            if success:
                ok_c += 1
            else:
                fail_c += 1
                print(f"  FAIL {handle} id={pid}: {err}", flush=True)
            if done % 200 == 0 or done == total:
                print(
                    f"  Toegepast: {done}/{total} (OK {ok_c}, mislukt {fail_c})...",
                    flush=True,
                )
        return success, err

    w = max(1, min(workers, total))
    with ThreadPoolExecutor(max_workers=w) as pool:
        list(pool.map(run_one, tasks))

    return ok_c, fail_c


def build_tasks_payload(
    source_csv: str,
    not_in_shop: list[str],
    missing_report: list[tuple[str, str, list[str]]],
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_csv": source_csv,
        "not_in_shop": not_in_shop,
        "tasks": [
            {"handle": h, "product_id": pid, "urls": urls}
            for h, pid, urls in missing_report
        ],
    }


def save_tasks_json(path: str, payload: dict[str, Any]) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_tasks_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def flatten_tasks_from_payload(payload: dict[str, Any]) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    for t in payload.get("tasks", []):
        pid = (t.get("product_id") or "").strip()
        h = (t.get("handle") or "").strip()
        for u in t.get("urls") or []:
            u = (u or "").strip()
            if u and pid:
                out.append((h, pid, u))
    return out
