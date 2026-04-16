#!/usr/bin/env python3
"""
Ontdubbel productafbeeldingen in Shopify per product (zelfde image-url na normalisatie).

Belangrijk:
- Standaard is dit een dry-run (alleen rapport, geen verwijderingen).
- Met --apply worden dubbele images echt verwijderd via Shopify REST API.
- De normalisatie gebruikt dezelfde logica als image-compare:
  Shopify copy-suffixes (bijv. _<uuid>) tellen als dezelfde afbeelding.

Voorbeelden:
  python3 scripts/shopify_dedupe_product_images.py --handle 00010000309
  python3 scripts/shopify_dedupe_product_images.py --tasks output/logs/shopify_missing_image_tasks.json
  python3 scripts/shopify_dedupe_product_images.py --limit-products 500
  python3 scripts/shopify_dedupe_product_images.py --apply
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

from modules.shopify_export_images_lib import norm_src  # noqa: E402
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

_REQUEST_TIMEOUT = (12, 120)
_DEFAULT_REPORT = PROJECT_ROOT / "output" / "logs" / "shopify_product_duplicate_images.csv"
_DEFAULT_TASKS = PROJECT_ROOT / "output" / "logs" / "shopify_missing_image_tasks.json"


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


def _retryable_get(sess: requests.Session, url: str, headers: dict[str, str]) -> requests.Response:
    for attempt in range(12):
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(2 + min(attempt, 8) * 0.35)
            continue
        if r.status_code >= 500:
            time.sleep(2 + attempt * 0.25)
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"GET retries uitgeput voor {url}")


def _retryable_delete(
    sess: requests.Session,
    url: str,
    headers: dict[str, str],
) -> tuple[bool, str]:
    for attempt in range(12):
        r = sess.delete(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code in (200, 204):
            return True, ""
        if r.status_code == 404:
            return True, "already_missing"
        if r.status_code == 429:
            time.sleep(2 + min(attempt, 8) * 0.35)
            continue
        if r.status_code >= 500:
            time.sleep(2 + attempt * 0.25)
            continue
        try:
            err = r.json()
        except Exception:
            err = r.text[:500]
        return False, str(err)
    return False, "te veel retries"


def _iter_products(
    shop: str,
    token: str,
    api_version: str,
) -> list[dict[str, Any]]:
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    out: list[dict[str, Any]] = []
    url = f"https://{shop}/admin/api/{api_version}/products.json?limit=250&fields=id,handle,images"
    while url:
        r = _retryable_get(sess, url, headers)
        data = r.json()
        out.extend(data.get("products") or [])
        if len(out) % 1000 == 0:
            print(f"  Producten opgehaald: {len(out)}", flush=True)
        url = _next_page_url(r.headers.get("Link"))
        if url:
            time.sleep(0.4)
    return out


def _fetch_products_by_handles(
    shop: str,
    token: str,
    api_version: str,
    handles: list[str],
) -> list[dict[str, Any]]:
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    out: list[dict[str, Any]] = []
    total = len(handles)
    for i, h in enumerate(handles, start=1):
        hq = quote(h, safe="")
        url = (
            f"https://{shop}/admin/api/{api_version}/products.json"
            f"?handle={hq}&fields=id,handle,images"
        )
        r = _retryable_get(sess, url, headers)
        products = r.json().get("products") or []
        if products:
            out.append(products[0])
        if i % 200 == 0 or i == total:
            print(f"  Handle-lookup: {i}/{total}", flush=True)
            time.sleep(0.15)
    return out


def _load_handles_from_tasks(path: Path) -> set[str]:
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    handles: set[str] = set()
    for task in payload.get("tasks") or []:
        h = normalize_shopify_product_handle(task.get("handle") or "")
        if h:
            handles.add(h)
    return handles


def _parse_handle_args(raw_handles: list[str] | None) -> set[str]:
    out: set[str] = set()
    for item in raw_handles or []:
        for part in str(item).replace("\n", ",").split(","):
            h = normalize_shopify_product_handle(part.strip())
            if h:
                out.add(h)
    return out


def _load_handles_file(path: Path) -> set[str]:
    out: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            h = normalize_shopify_product_handle(line.strip())
            if h:
                out.add(h)
    return out


def _build_duplicate_rows(products: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for p in products:
        pid = str(p.get("id") or "").strip()
        handle = normalize_shopify_product_handle(p.get("handle") or "")
        images = p.get("images") or []
        if not pid or not handle or len(images) < 2:
            continue

        # key: genormaliseerde src -> lijst image records in oorspronkelijke volgorde
        by_key: dict[str, list[dict[str, str]]] = {}
        for idx, img in enumerate(images):
            iid = str(img.get("id") or "").strip()
            src = (img.get("src") or "").strip()
            if not iid or not src:
                continue
            key = norm_src(src)
            if not key:
                continue
            rec = {
                "index": str(idx),
                "image_id": iid,
                "src": src,
                "norm_src": key,
            }
            by_key.setdefault(key, []).append(rec)

        for key, items in by_key.items():
            if len(items) < 2:
                continue
            keep = items[0]
            for dup in items[1:]:
                rows.append(
                    {
                        "handle": handle,
                        "product_id": pid,
                        "keep_image_id": keep["image_id"],
                        "delete_image_id": dup["image_id"],
                        "keep_src": keep["src"],
                        "delete_src": dup["src"],
                        "normalized_key": key,
                    }
                )
    return rows


def _write_report(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "handle",
        "product_id",
        "keep_image_id",
        "delete_image_id",
        "keep_src",
        "delete_src",
        "normalized_key",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _apply_deletes(
    shop: str,
    token: str,
    api_version: str,
    rows: list[dict[str, str]],
) -> tuple[int, int]:
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    ok = 0
    fail = 0
    for i, r in enumerate(rows, start=1):
        pid = r["product_id"]
        iid = r["delete_image_id"]
        url = f"https://{shop}/admin/api/{api_version}/products/{pid}/images/{iid}.json"
        success, err = _retryable_delete(sess, url, headers)
        if success:
            ok += 1
        else:
            fail += 1
            print(f"  FAIL {r['handle']} image_id={iid}: {err}", flush=True)
        if i % 200 == 0 or i == len(rows):
            print(f"  Verwerkt: {i}/{len(rows)} (OK {ok}, FAIL {fail})", flush=True)
    return ok, fail


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Ontdubbel productafbeeldingen in Shopify op basis van genormaliseerde image-src."
    )
    ap.add_argument(
        "--handle",
        action="append",
        default=[],
        help="Specifieke handle(s), komma-gescheiden mag ook. Meerdere --handle toegestaan.",
    )
    ap.add_argument(
        "--handles-file",
        default=None,
        metavar="PATH",
        help="Bestand met 1 handle per regel.",
    )
    ap.add_argument(
        "--tasks",
        default=str(_DEFAULT_TASKS),
        metavar="PATH",
        help="Gebruik handles uit taken-JSON (default: output/logs/shopify_missing_image_tasks.json).",
    )
    ap.add_argument(
        "--no-tasks",
        action="store_true",
        help="Negeer --tasks; scan alle producten (of alleen --handle/--handles-file).",
    )
    ap.add_argument(
        "--limit-products",
        type=int,
        default=0,
        metavar="N",
        help="Verwerk max N producten na filter (0 = geen limiet).",
    )
    ap.add_argument(
        "--report",
        default=str(_DEFAULT_REPORT),
        metavar="PATH",
        help=f"Rapport CSV (default: {_DEFAULT_REPORT}).",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Verwijder dubbele images echt (zonder deze flag: dry-run).",
    )
    ap.add_argument(
        "--max-deletes",
        type=int,
        default=0,
        metavar="N",
        help="Maximaal N duplicate image-rijen verwijderen (0 = geen limiet).",
    )
    args = ap.parse_args()
    if args.max_deletes < 0:
        print("--max-deletes moet >= 0 zijn.", file=sys.stderr)
        return 1

    token = config.SHOPIFY_ACCESS_TOKEN.strip()
    shop = config.SHOPIFY_SHOP_DOMAIN.strip()
    api_version = config.SHOPIFY_ADMIN_API_VERSION.strip()
    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", file=sys.stderr)
        return 1
    if not shop:
        print("SHOPIFY_SHOP_DOMAIN ontbreekt (.env).", file=sys.stderr)
        return 1
    if not api_version:
        print("SHOPIFY_ADMIN_API_VERSION ontbreekt (.env).", file=sys.stderr)
        return 1

    wanted_handles = _parse_handle_args(args.handle)
    if args.handles_file:
        wanted_handles |= _load_handles_file(Path(args.handles_file))
    if not args.no_tasks and args.tasks:
        tpath = Path(args.tasks)
        if tpath.is_file():
            wanted_handles |= _load_handles_from_tasks(tpath)
        else:
            print(f"Waarschuwing: tasks-bestand niet gevonden: {tpath}", flush=True)

    if wanted_handles:
        handles_list = sorted(wanted_handles)
        if len(handles_list) <= 500:
            print("Producten ophalen uit Shopify (gerichte handle-lookup)...", flush=True)
            products = _fetch_products_by_handles(shop, token, api_version, handles_list)
            print(f"Resultaat handle-lookup: {len(products)} producten", flush=True)
        else:
            print("Producten ophalen uit Shopify (volledige scan, veel handles)...", flush=True)
            products = _iter_products(shop, token, api_version)
            products = [
                p
                for p in products
                if normalize_shopify_product_handle(p.get("handle") or "") in wanted_handles
            ]
            print(f"Na handle-filter: {len(products)} producten", flush=True)
    else:
        print("Producten ophalen uit Shopify (volledige scan)...", flush=True)
        products = _iter_products(shop, token, api_version)
        print(f"Totaal producten opgehaald: {len(products)}", flush=True)

    if args.limit_products > 0:
        products = products[: args.limit_products]
        print(f"Na --limit-products: {len(products)} producten", flush=True)

    rows = _build_duplicate_rows(products)
    _write_report(Path(args.report), rows)

    unique_products = {(r["handle"], r["product_id"]) for r in rows}
    print(
        f"Duplicate image-rijen: {len(rows)} over {len(unique_products)} product(en).",
        flush=True,
    )
    print(f"Rapport: {args.report}", flush=True)

    if not rows:
        print("Geen dubbele productimages gevonden.", flush=True)
        return 0

    planned_rows = rows
    if args.max_deletes > 0 and len(rows) > args.max_deletes:
        planned_rows = rows[: args.max_deletes]
        print(
            f"Begrensd via --max-deletes: {len(planned_rows)} van {len(rows)} rij(en).",
            flush=True,
        )

    if not args.apply:
        print("Dry-run: geen afbeeldingen verwijderd. Gebruik --apply voor echte verwijdering.", flush=True)
        for r in planned_rows[:20]:
            print(
                f"  {r['handle']} | delete image_id={r['delete_image_id']} | keep={r['keep_image_id']}",
                flush=True,
            )
        if len(planned_rows) > 20:
            print(f"  ... en nog {len(planned_rows) - 20} rij(en).", flush=True)
        return 0

    print("Verwijderen van dubbele images...", flush=True)
    ok, fail = _apply_deletes(shop, token, api_version, planned_rows)
    print(f"Klaar: verwijderd OK={ok}, mislukt={fail}", flush=True)
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

