#!/usr/bin/env python3
"""
Vergelijk afbeeldingen in een Shopify-export-CSV (shopify_export_all_*.csv) met de live shop
en voeg ontbrekende productafbeeldingen toe (zelfde bron-URL’s als in de CSV).

Voorbeelden (vanaf projectroot):

  python3 scripts/shopify_sync_images_from_csv.py
  python3 scripts/shopify_sync_images_from_csv.py --csv output/products/shopify_export_all_20250101_120000.csv
  python3 scripts/shopify_sync_images_from_csv.py --apply
  python3 scripts/shopify_sync_images_from_csv.py --apply --limit 20
  python3 scripts/shopify_sync_images_from_csv.py --workers 8

Er worden alleen producten voor handles uit de CSV opgehaald (niet de hele catalogus).

Standaard is dry-run (geen writes). Vereist live Admin API (geen KTM_SKIP_SHOPIFY_API=1).

Scopes: producten lezen/schrijven; voor `src`-URL’s die extern zijn volstaat vaak `write_products`.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

from modules.shopify_client import _http_session  # noqa: E402
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
VERSION = config.SHOPIFY_ADMIN_API_VERSION
_REQUEST_TIMEOUT = (12, 120)


def _norm_src(url: str) -> str:
    if not url or not str(url).strip():
        return ""
    u = str(url).strip().split("?", 1)[0].rstrip("/")
    return u.lower()


def _latest_all_csv(products_dir: str) -> str | None:
    paths = glob.glob(os.path.join(products_dir, "shopify_export_all_*.csv"))
    if not paths:
        return None
    return max(paths, key=os.path.getmtime)


def _parse_csv_images(path: str) -> dict[str, list[str]]:
    """
    Handle (genormaliseerd) -> geordende lijst unieke Image Src-URL’s zoals in de CSV.
    """
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
            n = _norm_src(url)
            if not n or n in seen:
                continue
            seen.add(n)
            ordered.append(url.strip())
        if ordered:
            out[h] = ordered
    return out


def _session_for_thread() -> requests.Session:
    """Eigen Session per worker (thread-safe)."""
    s = requests.Session()
    s.trust_env = False
    return s


def _get_product_images_by_handle(
    sess: requests.Session,
    handle: str,
) -> tuple[set[str], str]:
    """
    GET products.json?handle=… — alleen dit product.
    Retourneert (genormaliseerde src-set, product-id) of (set(), '') als niet gevonden.
    """
    url = f"https://{SHOP}/admin/api/{VERSION}/products.json"
    params = {"handle": handle, "fields": "id,handle,images"}
    headers = {"X-Shopify-Access-Token": TOKEN}

    for attempt in range(10):
        r = sess.get(
            url,
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(2 + min(attempt, 5) * 0.4)
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
            s = img.get("src")
            if s:
                n = _norm_src(s)
                if n:
                    norms.add(n)
        return norms, pid_str

    return set(), ""


def _fetch_handle_maps_for_handles(
    handles: list[str],
    workers: int,
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """
    Live: alleen voor gegeven handles — handle -> image-src’s en product-id.
    Parallel met meerdere workers (eigen Session per thread).
    """
    norms_by_handle: dict[str, set[str]] = {}
    id_by_handle: dict[str, str] = {}
    total = len(handles)
    if total == 0:
        return norms_by_handle, id_by_handle

    lock = threading.Lock()
    done = 0

    def run_one(h: str) -> tuple[str, set[str], str]:
        nonlocal done
        sess = _session_for_thread()
        norms, pid = _get_product_images_by_handle(sess, h)
        with lock:
            done += 1
            if done % 250 == 0 or done == total:
                print(f"  Live opgehaald: {done}/{total} handles...", flush=True)
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


def _post_product_image(
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


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Vergelijk Image Src in shopify_export_all-CSV met live Shopify en voeg ontbrekende images toe."
    )
    ap.add_argument(
        "--csv",
        metavar="PATH",
        help=f"Pad naar shopify_export_all_*.csv (default: nieuwste in {config.PRODUCTS_OUTPUT_DIR})",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Ontbrekende images echt toevoegen (default: alleen rapport)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        metavar="N",
        default=0,
        help="Alleen eerste N handles uit de CSV verwerken (na sortering)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=6,
        metavar="N",
        help="Parallelle API-requests voor handles uit de CSV (default: 6)",
    )
    args = ap.parse_args()

    if os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        print(
            "KTM_SKIP_SHOPIFY_API is gezet — dit script heeft live Shopify nodig.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    if not TOKEN:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", file=sys.stderr)
        raise SystemExit(1)

    csv_path = args.csv or _latest_all_csv(config.PRODUCTS_OUTPUT_DIR)
    if not csv_path or not os.path.isfile(csv_path):
        print(
            "Geen CSV: geef --csv pad of zet shopify_export_all_*.csv in output/products/.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(f"CSV: {csv_path}", flush=True)
    expected_by_handle = _parse_csv_images(csv_path)
    if not expected_by_handle:
        print("Geen enkele rij met Image Src in deze CSV.", flush=True)
        return

    handles_sorted = sorted(expected_by_handle.keys())
    if args.limit and args.limit > 0:
        handles_sorted = handles_sorted[: args.limit]
        expected_by_handle = {h: expected_by_handle[h] for h in handles_sorted}

    print(
        f"Handles in CSV (met ten minste één image): {len(expected_by_handle)}",
        flush=True,
    )
    print(
        f"Live producten ophalen (alleen deze handles, workers={args.workers})...",
        flush=True,
    )
    live_norms, live_id_by_handle = _fetch_handle_maps_for_handles(
        handles_sorted,
        args.workers,
    )

    missing_report: list[tuple[str, str, list[str]]] = []
    not_in_shop: list[str] = []
    for handle in handles_sorted:
        urls = expected_by_handle[handle]
        if handle not in live_id_by_handle:
            not_in_shop.append(handle)
            continue
        have = live_norms.get(handle, set())
        missing = [u for u in urls if _norm_src(u) not in have]
        if missing:
            missing_report.append((handle, live_id_by_handle[handle], missing))

    if not_in_shop:
        print(
            f"\nHandles in CSV maar niet gevonden in live productlijst ({len(not_in_shop)}):",
            flush=True,
        )
        for h in not_in_shop[:50]:
            print(f"  {h}", flush=True)
        if len(not_in_shop) > 50:
            print(f"  ... en {len(not_in_shop) - 50} meer", flush=True)

    if not missing_report:
        print("\nGeen ontbrekende images t.o.v. CSV (voor verwerkte handles).", flush=True)
        return

    print(
        f"\nOntbrekende images: {len(missing_report)} producten, "
        f"{sum(len(x[2]) for x in missing_report)} URL-totalen.",
        flush=True,
    )
    for handle, pid, miss in missing_report[:30]:
        print(f"  {handle} (product id {pid or '?'}): {len(miss)} image(s)", flush=True)
        for u in miss[:3]:
            if len(u) > 100:
                print(f"    - {u[:100]}...", flush=True)
            else:
                print(f"    - {u}", flush=True)
        if len(miss) > 3:
            print(f"    ... {len(miss) - 3} meer", flush=True)
    if len(missing_report) > 30:
        print(f"  ... en {len(missing_report) - 30} producten meer", flush=True)

    if not args.apply:
        print(
            "\nDry-run: geen wijzigingen. Voer opnieuw uit met --apply om toe te voegen.",
            flush=True,
        )
        return

    print("\n--apply: images toevoegen...", flush=True)
    sess = _http_session()
    ok = 0
    fail = 0
    for handle, pid, miss in missing_report:
        if not pid:
            print(f"  SKIP {handle}: geen product id in index", flush=True)
            fail += len(miss)
            continue
        for src in miss:
            for attempt in range(5):
                success, err = _post_product_image(sess, pid, src)
                if success:
                    ok += 1
                    have = live_norms.setdefault(handle, set())
                    have.add(_norm_src(src))
                    break
                if err == "429":
                    time.sleep(2 + attempt)
                    continue
                if err.startswith("HTTP 5"):
                    time.sleep(3)
                    continue
                print(f"  FAIL {handle} id={pid}: {err}", flush=True)
                fail += 1
                break
            else:
                print(f"  FAIL {handle} id={pid}: te veel retries", flush=True)
                fail += 1
            time.sleep(0.35)

    print(f"\nKlaar: toegevoegd OK={ok}, mislukt={fail}.", flush=True)


if __name__ == "__main__":
    main()
