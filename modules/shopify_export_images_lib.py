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
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import requests

import config
from modules.image_manager import ensure_image, load_cache, save_cache_safe
from modules.image_resolve import build_basename_index, resolve_local_image
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
              id
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
_SHOPIFY_FILE_COPY_SUFFIX_RE = re.compile(
    r"_(?:"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"|[0-9a-f]{12,64}"
    r")$",
    re.IGNORECASE,
)
_FALLBACK_IMAGE_BASENAME_RE = re.compile(
    r"^pho_fallback_fallback_no_picture(?:__.*)?\.(?:jpg|jpeg|png|webp)$",
    re.IGNORECASE,
)


def default_tasks_path() -> str:
    return os.path.join(config.LOG_OUTPUT_DIR, DEFAULT_TASKS_BASENAME)


def _export_url_clean_basename(url: str) -> str:
    """
    Laatste URL-segment met Shopify-kopie-suffix uit de stam gehaald (zelfde regels als norm_src).
    Gebruikt voor match met bestanden onder input/.
    """
    u = str(url or "").strip().split("?", 1)[0].rstrip("/")
    if not u:
        return ""
    slash = u.rfind("/")
    base = unquote(u[slash + 1 :] if slash >= 0 else u)
    stem, ext = os.path.splitext(base)
    if stem:
        stem = _SHOPIFY_FILE_COPY_SUFFIX_RE.sub("", stem)
    return f"{stem}{ext}"


def norm_src(url: str) -> str:
    if not url or not str(url).strip():
        return ""
    clean_base = _export_url_clean_basename(url)
    return clean_base.lower()


def is_fallback_image_url(url: str) -> bool:
    key = norm_src(url)
    if not key:
        return False
    return bool(_FALLBACK_IMAGE_BASENAME_RE.match(key))


def latest_product_csv(products_dir: str) -> str | None:
    """Nieuwste delta-export. Oudere all-bestanden blijven bruikbaar tot de volgende run."""
    for pattern in ("shopify_export_delta_*.csv", "shopify_export_all_*.csv"):
        paths = glob.glob(os.path.join(products_dir, pattern))
        if paths:
            return max(paths, key=os.path.getmtime)
    return None


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
            if is_fallback_image_url(src):
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


def parse_csv_images_split(
    path: str,
) -> dict[str, tuple[list[str], list[str]]]:
    """
    Handle (genormaliseerd) -> (echte Image Src-URL's, fallback-URL's), beide geordend en uniek.
    """
    by_pos: dict[str, list[tuple[int, str, bool]]] = defaultdict(list)
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
            by_pos[h].append((ipos, src, is_fallback_image_url(src)))

    out: dict[str, tuple[list[str], list[str]]] = {}
    for h, pairs in by_pos.items():
        pairs.sort(key=lambda x: (x[0], x[1]))
        seen_real: set[str] = set()
        seen_fb: set[str] = set()
        real: list[str] = []
        fallback: list[str] = []
        for _, url, is_fb in pairs:
            n = norm_src(url)
            if not n:
                continue
            u = url.strip()
            if is_fb:
                if n in seen_fb:
                    continue
                seen_fb.add(n)
                fallback.append(u)
            else:
                if n in seen_real:
                    continue
                seen_real.add(n)
                real.append(u)
        if real or fallback:
            out[h] = (real, fallback)
    return out


def live_norms_are_fallback_only(norms: set[str]) -> bool:
    """True als er live media is en elk item de placeholder-fallback is."""
    if not norms:
        return False
    for n in norms:
        if not _FALLBACK_IMAGE_BASENAME_RE.match(n):
            return False
    return True


def session_for_thread() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def get_product_images_by_handle(
    sess: requests.Session,
    handle: str,
) -> tuple[set[str], str, list[dict[str, str]]]:
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
            return set(), "", []
        p = products[0]
        pid = p.get("id")
        pid_str = str(int(pid)) if isinstance(pid, (int, float)) else str(pid or "")
        norms: set[str] = set()
        images: list[dict[str, str]] = []
        ordered = sorted(
            p.get("images") or [],
            key=lambda im: int(im.get("position") or 0),
        )
        for img in ordered:
            rec = _live_image_record(img.get("id"), img.get("src"))
            if not rec:
                continue
            images.append(rec)
            norms.add(rec["norm"])
        return norms, pid_str, images

    return set(), "", []


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


def _live_image_record(image_id: Any, src: Any) -> dict[str, str] | None:
    """REST- of GraphQL-image → {id, src, norm}. Zonder id of bestandsnaam: overslaan."""
    iid = _gid_numeric(str(image_id or ""))
    raw = str(src or "").strip()
    ns = norm_src(raw)
    if not iid or not raw or not ns:
        return None
    return {"id": iid, "src": raw, "norm": ns}


def _parse_graphql_product_node(
    node: dict[str, Any],
) -> tuple[str, set[str], str, list[dict[str, str]]]:
    handle = normalize_shopify_product_handle(node.get("handle") or "")
    pid = _gid_numeric(str(node.get("id") or ""))
    norms: set[str] = set()
    images: list[dict[str, str]] = []
    for e in ((node.get("images") or {}).get("edges")) or []:
        n = (e or {}).get("node") or {}
        url = n.get("url") or n.get("originalSrc")
        rec = _live_image_record(n.get("id"), url)
        if not rec:
            continue
        images.append(rec)
        norms.add(rec["norm"])
    return handle, norms, pid, images


def _graphql_products_batch(
    sess: requests.Session,
    query_string: str,
) -> dict[str, tuple[set[str], str, list[dict[str, str]]]]:
    """Zoekquery → handle -> (norms, product_id, images in galerijvolgorde)."""
    out: dict[str, tuple[set[str], str, list[dict[str, str]]]] = {}
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
        handle, norms, pid, images = _parse_graphql_product_node(node)
        if handle and pid:
            out[handle] = (norms, pid, images)
    return out


def fetch_handle_maps_rest_only(
    handles: list[str],
    workers: int,
    *,
    images_out: dict[str, list[dict[str, str]]] | None = None,
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """Eén REST GET per handle (langzaam bij grote sets)."""
    norms_by_handle: dict[str, set[str]] = {}
    id_by_handle: dict[str, str] = {}
    total = len(handles)
    if total == 0:
        return norms_by_handle, id_by_handle

    lock = threading.Lock()
    done = 0

    def run_one(h: str) -> tuple[str, set[str], str, list[dict[str, str]]]:
        nonlocal done
        sess = session_for_thread()
        norms, pid, images = get_product_images_by_handle(sess, h)
        with lock:
            done += 1
            if done % 250 == 0 or done == total:
                print(f"  Live opgehaald (REST): {done}/{total} handles...", flush=True)
        return h, norms, pid, images

    w = max(1, min(workers, total))
    with ThreadPoolExecutor(max_workers=w) as pool:
        futures = [pool.submit(run_one, h) for h in handles]
        for fut in as_completed(futures):
            h, norms, pid, images = fut.result()
            if pid:
                id_by_handle[h] = pid
                norms_by_handle[h] = norms
                if images_out is not None:
                    images_out[h] = images

    return norms_by_handle, id_by_handle


def fetch_handle_maps_for_handles(
    handles: list[str],
    rest_workers: int = 8,
    *,
    graphql_batch: int = 25,
    fetch_workers: int = 12,
    rest_only: bool = False,
    images_out: dict[str, list[dict[str, str]]] | None = None,
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
        return fetch_handle_maps_rest_only(handles, rest_workers, images_out=images_out)

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
    merged: dict[str, tuple[set[str], str, list[dict[str, str]]]] = {}

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

    for h, (norms, pid, images) in merged.items():
        if pid:
            id_by_handle[h] = pid
            norms_by_handle[h] = norms
            if images_out is not None:
                images_out[h] = images

    missing = [h for h in uniq if h not in id_by_handle]
    if missing:
        print(
            f"  REST fallback voor {len(missing)} handle(s) (niet in GraphQL-resultaat)…",
            flush=True,
        )
        n_rest, id_rest = fetch_handle_maps_rest_only(
            missing, rest_workers, images_out=images_out
        )
        norms_by_handle.update(n_rest)
        id_by_handle.update(id_rest)

    print(f"  Live opgehaald: {len(id_by_handle)}/{total} handles met product-id.", flush=True)
    return norms_by_handle, id_by_handle


def compare_csv_missing_images_against_live(
    csv_path: str,
    *,
    limit: int = 0,
    rest_workers: int = 8,
    graphql_batch: int = 25,
    fetch_workers: int = 12,
    rest_only: bool = False,
) -> tuple[list[tuple[str, str, list[str]]], list[str], dict[str, list[str]]]:
    """
    Lees ``csv_path`` (Handle + Image Src), haal live media per handle op, bouw hetzelfde
    ``missing_report`` / ``not_in_shop`` als ``shopify_compare_export_images.py``.

    Retourneert ``(missing_report, not_in_shop, expected_by_handle)``.
    ``expected_by_handle`` is na eventuele ``limit`` ingekort (gesorteerde handles).
    """
    expected_by_handle = parse_csv_images(csv_path)
    if not expected_by_handle:
        return [], [], {}
    handles_sorted = sorted(expected_by_handle.keys())
    if limit and limit > 0:
        handles_sorted = handles_sorted[:limit]
        expected_by_handle = {h: expected_by_handle[h] for h in handles_sorted}

    print(
        f"Handles in CSV (met ten minste één image): {len(expected_by_handle)}",
        flush=True,
    )
    print("Live producten ophalen (alleen deze handles)...", flush=True)

    live_norms, live_id_by_handle = fetch_handle_maps_for_handles(
        handles_sorted,
        rest_workers,
        graphql_batch=graphql_batch,
        fetch_workers=fetch_workers,
        rest_only=rest_only,
    )

    missing_report: list[tuple[str, str, list[str]]] = []
    not_in_shop: list[str] = []
    for handle in handles_sorted:
        urls = expected_by_handle[handle]
        if handle not in live_id_by_handle:
            not_in_shop.append(handle)
            continue
        have = live_norms.get(handle, set())
        missing = [u for u in urls if norm_src(u) not in have]
        if missing:
            missing_report.append((handle, live_id_by_handle[handle], missing))

    return missing_report, not_in_shop, expected_by_handle


_SHOPIFY_EXTRA_VERSION_RE = re.compile(r"__v\d+$", re.IGNORECASE)


def expected_norm_for_live(live_norm: str, expected: set[str]) -> str | None:
    """
    Live bestandsnaam hoort bij een XML-foto.

    Shopify zet bij een dubbele upload soms nog een ``__vN`` achter de KTM-naam
    (``…__v1__v1.jpg``). Eén zo'n suffix telt als dezelfde XML-foto.
    ``…__v1`` en ``…__v2`` blijven verschillende foto's.
    """
    key = (live_norm or "").strip().lower()
    if not key or not expected:
        return None
    if key in expected:
        return key
    stem, ext = os.path.splitext(key)
    collapsed = f"{_SHOPIFY_EXTRA_VERSION_RE.sub('', stem, count=1)}{ext}"
    if collapsed != key and collapsed in expected:
        return collapsed
    return None


def plan_product_gallery(
    expected_urls: list[str],
    live_images: list[dict[str, str]],
) -> dict[str, Any]:
    """
    XML-fotolijst tegen de live galerij.

    Lege ``expected_urls`` verwijdert niets. Foto's zonder id blijven staan.
    """
    expected_norms: list[str] = []
    seen: set[str] = set()
    url_by_norm: dict[str, str] = {}
    for url in expected_urls:
        n = norm_src(url)
        if not n or n in seen:
            continue
        seen.add(n)
        expected_norms.append(n)
        url_by_norm[n] = url.strip()

    keep_id_by_norm: dict[str, str] = {}
    delete_ids: list[str] = []
    delete_srcs: list[str] = []
    if expected_norms:
        expected_set = set(expected_norms)
        for img in live_images:
            iid = str(img.get("id") or "")
            matched = expected_norm_for_live(str(img.get("norm") or ""), expected_set)
            if matched and matched not in keep_id_by_norm and iid:
                keep_id_by_norm[matched] = iid
                continue
            if not iid:
                continue
            delete_ids.append(iid)
            delete_srcs.append(str(img.get("src") or ""))

    missing_urls = [url_by_norm[n] for n in expected_norms if n not in keep_id_by_norm]
    return {
        "expected_norms": expected_norms,
        "missing_urls": missing_urls,
        "delete_ids": delete_ids,
        "delete_srcs": delete_srcs,
        "keep_id_by_norm": keep_id_by_norm,
        "live_images": list(live_images),
    }


def gallery_target_order(plan: dict[str, Any], new_ids_by_norm: dict[str, str]) -> list[str]:
    keep = plan.get("keep_id_by_norm") or {}
    order: list[str] = []
    for norm in plan.get("expected_norms") or []:
        iid = keep.get(norm) or new_ids_by_norm.get(norm)
        if iid:
            order.append(str(iid))
    return order


def gallery_predicted_remaining(
    plan: dict[str, Any],
    new_ids_by_norm: dict[str, str],
) -> list[str]:
    """Volgorde na deletes, met nieuw toegevoegde foto's achteraan."""
    delete = {str(i) for i in (plan.get("delete_ids") or [])}
    keep = plan.get("keep_id_by_norm") or {}
    remaining = [
        str(img["id"])
        for img in plan.get("live_images") or []
        if str(img.get("id") or "") and str(img["id"]) not in delete
    ]
    for norm in plan.get("expected_norms") or []:
        iid = new_ids_by_norm.get(norm)
        if iid and norm not in keep:
            remaining.append(str(iid))
    return remaining


def gallery_needs_reorder(plan: dict[str, Any], new_ids_by_norm: dict[str, str]) -> bool:
    target = gallery_target_order(plan, new_ids_by_norm)
    if not target:
        return False
    return gallery_predicted_remaining(plan, new_ids_by_norm) != target


def gallery_replace_is_safe(plan: dict[str, Any], new_ids_by_norm: dict[str, str]) -> bool:
    """Verwijder pas als de eerste XML-foto echt op het product staat."""
    norms = plan.get("expected_norms") or []
    if not norms:
        return False
    first = norms[0]
    keep = plan.get("keep_id_by_norm") or {}
    return first in keep or first in new_ids_by_norm


def compare_csv_gallery_against_live(
    csv_path: str,
    *,
    limit: int = 0,
    rest_workers: int = 8,
    graphql_batch: int = 25,
    fetch_workers: int = 12,
    rest_only: bool = False,
) -> tuple[
    list[tuple[str, str, list[str]]],
    list[str],
    dict[str, list[str]],
    dict[str, dict[str, Any]],
]:
    """
    Zelfde compare als ``compare_csv_missing_images_against_live``, plus een plan
    per handle waar de galerij niet gelijk is aan de CSV-foto's.

    ``plans[handle]`` bevat ``product_id`` en de velden van ``plan_product_gallery``.
    """
    expected_by_handle = parse_csv_images(csv_path)
    if not expected_by_handle:
        return [], [], {}, {}
    handles_sorted = sorted(expected_by_handle.keys())
    if limit and limit > 0:
        handles_sorted = handles_sorted[:limit]
        expected_by_handle = {h: expected_by_handle[h] for h in handles_sorted}

    print(
        f"Handles in CSV (met ten minste één image): {len(expected_by_handle)}",
        flush=True,
    )
    print("Live producten ophalen (alleen deze handles)...", flush=True)

    images_by_handle: dict[str, list[dict[str, str]]] = {}
    _live_norms, live_id_by_handle = fetch_handle_maps_for_handles(
        handles_sorted,
        rest_workers,
        graphql_batch=graphql_batch,
        fetch_workers=fetch_workers,
        rest_only=rest_only,
        images_out=images_by_handle,
    )

    missing_report: list[tuple[str, str, list[str]]] = []
    not_in_shop: list[str] = []
    plans: dict[str, dict[str, Any]] = {}
    for handle in handles_sorted:
        urls = expected_by_handle[handle]
        if handle not in live_id_by_handle:
            not_in_shop.append(handle)
            continue
        plan = plan_product_gallery(urls, images_by_handle.get(handle, []))
        pid = live_id_by_handle[handle]
        if plan["missing_urls"]:
            missing_report.append((handle, pid, plan["missing_urls"]))
        if plan["missing_urls"] or plan["delete_ids"] or gallery_needs_reorder(plan, {}):
            plans[handle] = {"product_id": pid, **plan}

    return missing_report, not_in_shop, expected_by_handle, plans


def post_product_image(
    sess: requests.Session,
    product_id: str,
    src: str,
) -> tuple[bool, str, str]:
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
        return False, "429", ""
    if r.status_code >= 500:
        return False, f"HTTP {r.status_code}", ""
    if r.status_code not in (200, 201):
        try:
            err = r.json()
        except Exception:
            err = r.text[:500]
        return False, str(err), ""
    image_id = ""
    try:
        image_id = str(((r.json() or {}).get("image") or {}).get("id") or "")
    except Exception:
        image_id = ""
    return True, "", image_id


def _is_product_locked_error(err: str) -> bool:
    e = (err or "").lower()
    return "currently being modified" in e or "being modified" in e


def post_product_image_retries(
    sess: requests.Session,
    product_id: str,
    src: str,
) -> tuple[bool, str, str]:
    max_attempts = 22
    for attempt in range(max_attempts):
        ok, err, image_id = post_product_image(sess, product_id, src)
        if ok:
            return True, "", image_id
        if err == "429":
            time.sleep(2 + min(attempt, 8) * 0.35)
            continue
        if err.startswith("HTTP 5"):
            time.sleep(2 + attempt * 0.25)
            continue
        if _is_product_locked_error(err):
            time.sleep(0.85 + min(attempt, 12) * 0.45)
            continue
        return False, err, ""
    return False, "te veel retries", ""


_product_post_locks: dict[str, threading.Lock] = {}
_product_post_locks_guard = threading.Lock()


def _product_post_lock(product_id: str) -> threading.Lock:
    with _product_post_locks_guard:
        if product_id not in _product_post_locks:
            _product_post_locks[product_id] = threading.Lock()
        return _product_post_locks[product_id]


def apply_missing_images_parallel(
    tasks: list[tuple[str, str, str]],
    workers: int,
    *,
    input_dir: str | None = None,
    prefer_input: bool = True,
    added_ids: dict[tuple[str, str], str] | None = None,
) -> tuple[int, int]:
    total = len(tasks)
    if total == 0:
        return 0, 0

    cache: dict[str, Any] | None = None
    input_root: Path | None = None
    by_exact: dict[str, list[Path]] | None = None
    by_lower: dict[str, list[Path]] | None = None
    cache_lock = threading.Lock()

    if prefer_input:
        cache = load_cache()
        input_root = Path(input_dir or config.INPUT_DIR).resolve()
        by_exact, by_lower = build_basename_index(input_root)
        print(
            f"  Lokale bron: {input_root} ({len(by_exact)} unieke bestandsnamen). "
            "Per taak: match op CSV-URL → bestand in deze map → upload/resolve via image_cache → product koppelen.",
            flush=True,
        )

    lock = threading.Lock()
    done = 0
    ok_c = 0
    fail_c = 0

    def run_one(item: tuple[str, str, str]) -> tuple[bool, str]:
        nonlocal done, ok_c, fail_c
        handle, pid, src = item
        sess = session_for_thread()
        attach_src = src

        if (
            prefer_input
            and cache is not None
            and input_root is not None
            and by_exact is not None
            and by_lower is not None
        ):
            ref = _export_url_clean_basename(src)
            if ref:
                local_path = resolve_local_image(
                    ref, input_root, by_exact, by_lower
                )
                if local_path is not None:
                    with cache_lock:
                        resolved, _ = ensure_image(
                            local_path.name,
                            local_path,
                            cache,
                            strict_delta=True,
                        )
                    if resolved:
                        attach_src = resolved
                    else:
                        with lock:
                            print(
                                f"  SKIP-POST {handle} id={pid}: lokaal {local_path.name!r} maar "
                                "ensure_image (Shopify lookup/upload) leverde geen URL — "
                                "geen POST met (meestal dode) CSV-CDN-URL.",
                                flush=True,
                            )
                            done += 1
                            fail_c += 1
                            if done % 200 == 0 or done == total:
                                print(
                                    f"  Toegepast: {done}/{total} (OK {ok_c}, mislukt {fail_c})...",
                                    flush=True,
                                )
                        return False, "ensure_image geen URL"

        with _product_post_lock(pid):
            success, err, image_id = post_product_image_retries(sess, pid, attach_src)

        with lock:
            done += 1
            if success:
                ok_c += 1
                if added_ids is not None and image_id:
                    key = norm_src(src)
                    if key:
                        added_ids[(handle, key)] = str(image_id)
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

    if prefer_input and cache is not None:
        save_cache_safe(cache)

    return ok_c, fail_c


def _retry_image_write(call) -> tuple[bool, str]:
    """call() -> (ok, err). Zelfde retries als een product-image POST."""
    for attempt in range(22):
        ok, err = call()
        if ok:
            return True, ""
        if err == "429":
            time.sleep(2 + min(attempt, 8) * 0.35)
            continue
        if err.startswith("HTTP 5"):
            time.sleep(2 + attempt * 0.25)
            continue
        if _is_product_locked_error(err):
            time.sleep(0.85 + min(attempt, 12) * 0.45)
            continue
        return False, err
    return False, "te veel retries"


def delete_product_image(
    sess: requests.Session,
    product_id: str,
    image_id: str,
) -> tuple[bool, str]:
    url = f"https://{SHOP}/admin/api/{VERSION}/products/{product_id}/images/{image_id}.json"
    headers = {"X-Shopify-Access-Token": TOKEN}
    r = sess.delete(
        url,
        headers=headers,
        timeout=_REQUEST_TIMEOUT,
        proxies={"http": None, "https": None},
    )
    if r.status_code in (200, 204, 404):
        return True, ""
    if r.status_code == 429:
        return False, "429"
    if r.status_code >= 500:
        return False, f"HTTP {r.status_code}"
    try:
        err = r.json()
    except Exception:
        err = r.text[:500]
    return False, str(err)


def set_product_image_position(
    sess: requests.Session,
    product_id: str,
    image_id: str,
    position: int,
) -> tuple[bool, str]:
    url = f"https://{SHOP}/admin/api/{VERSION}/products/{product_id}/images/{image_id}.json"
    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }
    body = {"image": {"id": int(image_id), "position": position}}
    r = sess.put(
        url,
        headers=headers,
        json=body,
        timeout=_REQUEST_TIMEOUT,
        proxies={"http": None, "https": None},
    )
    if r.status_code in (200, 201):
        return True, ""
    if r.status_code == 429:
        return False, "429"
    if r.status_code >= 500:
        return False, f"HTTP {r.status_code}"
    try:
        err = r.json()
    except Exception:
        err = r.text[:500]
    return False, str(err)


def apply_gallery_cleanup(
    plans: dict[str, dict[str, Any]],
    added_ids: dict[tuple[str, str], str],
    workers: int,
) -> tuple[int, int, int]:
    """
    Verwijder foto's die niet meer in de XML staan en zet de XML-volgorde.

    Returns ``(verwijderd_ok, verwijderd_fail, herschikt)``.
    Slaat een handle over als de eerste XML-foto niet op het product staat.
    """
    items = list(plans.items())
    total = len(items)
    if total == 0:
        return 0, 0, 0

    lock = threading.Lock()
    done = 0
    del_ok = 0
    del_fail = 0
    reordered = 0

    def run_one(item: tuple[str, dict[str, Any]]) -> tuple[int, int, int]:
        nonlocal done, del_ok, del_fail, reordered
        handle, plan = item
        pid = str(plan.get("product_id") or "")
        new_ids = {
            norm: image_id
            for (h, norm), image_id in added_ids.items()
            if h == handle and image_id
        }
        ok_n = fail_n = re_n = 0
        if not pid or not gallery_replace_is_safe(plan, new_ids):
            if plan.get("delete_ids"):
                print(
                    f"  SKIP-DELETE {handle}: eerste XML-foto staat niet op het product",
                    flush=True,
                )
            with lock:
                done += 1
            return 0, 0, 0

        sess = session_for_thread()
        with _product_post_lock(pid):
            for image_id in plan.get("delete_ids") or []:
                success, err = _retry_image_write(
                    lambda iid=str(image_id): delete_product_image(sess, pid, iid)
                )
                if success:
                    ok_n += 1
                else:
                    fail_n += 1
                    print(
                        f"  FAIL-DELETE {handle} id={pid} image={image_id}: {err}",
                        flush=True,
                    )
            if plan.get("delete_ids") or gallery_needs_reorder(plan, new_ids):
                order = gallery_target_order(plan, new_ids)
                position_ok = True
                for position, image_id in enumerate(order, start=1):
                    success, err = _retry_image_write(
                        lambda iid=str(image_id), pos=position: set_product_image_position(
                            sess, pid, iid, pos
                        )
                    )
                    if not success:
                        position_ok = False
                        print(
                            f"  FAIL-POSITIE {handle} id={pid} image={image_id}: {err}",
                            flush=True,
                        )
                        break
                if position_ok and order:
                    re_n = 1

        with lock:
            done += 1
            del_ok += ok_n
            del_fail += fail_n
            reordered += re_n
            if done % 200 == 0 or done == total:
                print(
                    f"  Galerij: {done}/{total} "
                    f"(verwijderd {del_ok}, mislukt {del_fail}, herschikt {reordered})...",
                    flush=True,
                )
        return ok_n, fail_n, re_n

    w = max(1, min(workers, total))
    with ThreadPoolExecutor(max_workers=w) as pool:
        list(pool.map(run_one, items))

    return del_ok, del_fail, reordered


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
