#!/usr/bin/env python3
"""
CSV naar stdout: producten rond **gedeelde SKU’s** met een “familie”-handle op **x**.

**Kwalificatie van een SKU (bijv. 3KI230045800):**

- Die SKU komt op **minstens twee verschillende producten** voor (variant-SKU’s), én
- Er is **minstens één** product met: handle eindigt op **x** (bv. `3ki23004580x`),
  **precies één** variant, en die variant heeft precies deze SKU.

**Output:** elk product dat een variant heeft met zo’n gekwalificeerde SKU — dus zowel het
**x**-single-variant product als het **andere** product (zelfde SKU, andere handle).

Valt af o.a.: handles die op x eindigen maar **meerdere** varianten (`3pw24000500x`, …).

Uitvoer: CSV (stdout); voortgang naar stderr.

  python3 scripts/shopify_list_single_variant_sku_suffix_x.py > out.csv

Vereist: SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN in .env (zie .env.example).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
import tempfile
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
from modules.pricing_loader import normalize_sku_key  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)
_REQUEST_TIMEOUT_LONG = (12, 600)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        variants {
          edges {
            node {
              id
              sku
              title
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation KtmBulkHandleSuffixSingleVariant {
  bulkOperationRunQuery(
    query: BULK_QUERY_PLACEHOLDER
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

_GQL_POLL = """
query KtmPollBulk($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      status
      errorCode
      objectCount
      url
      partialDataUrl
    }
  }
}
"""


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _graphql_post(
    sess: requests.Session,
    query: str,
    variables: dict | None = None,
) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    last: dict = {}
    for attempt in range(25):
        r = sess.post(
            _GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": TOKEN,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT_LONG,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return last
    return last


def _gid_numeric(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]


def _handle_ends_with_suffix(handle: str, suffix: str) -> bool:
    h = (handle or "").strip().lower()
    s = (suffix or "").strip().lower()
    if not s:
        return False
    return h.endswith(s)


def _is_x_singleton_product(
    pm: dict, *, handle_suffix: str, sku_key: str
) -> bool:
    """Eén variant, handle eindigt op suffix, en die variant heeft SKU sku_key."""
    handle = (pm.get("handle") or "").strip()
    if not _handle_ends_with_suffix(handle, handle_suffix):
        return False
    variants = pm.get("variants") or []
    if len(variants) != 1:
        return False
    v0 = variants[0]
    if normalize_sku_key(v0.get("sku")) != sku_key:
        return False
    return True


def _download_bulk_to_temp(sess: requests.Session, url: str) -> str:
    with sess.get(
        url,
        stream=True,
        timeout=_REQUEST_TIMEOUT_LONG,
        proxies={"http": None, "https": None},
    ) as r:
        r.raise_for_status()
        fd, path = tempfile.mkstemp(suffix=".jsonl", prefix="shopify_bulk_")
        try:
            with os.fdopen(fd, "wb") as out:
                shutil.copyfileobj(r.raw, out)
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
    return path


def _parse_jsonl_build_index(
    path: str,
) -> tuple[dict[str, dict], dict[str, set[str]]]:
    """
    Retourneert (product_meta, sku_to_product_gids).
    product_meta[gid] = {handle, title, status, variants: [{sku, id}]}
    """
    product_meta: dict[str, dict] = {}
    sku_to_products: dict[str, set[str]] = defaultdict(set)

    current_product: dict | None = None
    current_pid: str | None = None
    variant_buf: list[dict] = []

    line_n = 0

    def flush() -> None:
        nonlocal current_product, current_pid, variant_buf
        if not current_product or not current_pid:
            variant_buf = []
            return
        gid = current_pid
        handle = (current_product.get("handle") or "").strip()
        title = (current_product.get("title") or "").strip()
        st = (current_product.get("status") or "").strip().upper()
        variants_out: list[dict] = []
        for v in variant_buf:
            raw_sku = (v.get("sku") or "").strip()
            sk = normalize_sku_key(raw_sku)
            vid = (v.get("id") or "").strip()
            variants_out.append(
                {"sku": raw_sku, "sku_key": sk, "variant_gid": vid}
            )
            if sk:
                sku_to_products[sk].add(gid)
        product_meta[gid] = {
            "handle": handle,
            "title": title,
            "status": st,
            "variants": variants_out,
        }
        variant_buf = []

    with open(path, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            if not raw_line.strip():
                continue
            line_n += 1
            if line_n % 250_000 == 0:
                print(f"  ... {line_n} regels gelezen", file=sys.stderr, flush=True)
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError:
                continue

            gid = (obj.get("id") or "").strip()
            if "/ProductVariant/" in gid:
                parent = (obj.get("__parentId") or "").strip()
                if parent == current_pid:
                    variant_buf.append(obj)
                continue

            if "/Product/" in gid and "/ProductVariant/" not in gid:
                flush()
                current_product = obj
                current_pid = gid
                variant_buf = []
                continue

    flush()
    return product_meta, sku_to_products


def _qualifying_skus(
    product_meta: dict[str, dict],
    sku_to_products: dict[str, set[str]],
    *,
    handle_suffix: str,
) -> set[str]:
    """
    SKU’s die op ≥2 producten voorkomen én waarvoor minstens één product een
    x-handle + één variant met precies die SKU is.
    """
    out: set[str] = set()
    for sku_key, pids in sku_to_products.items():
        if len(pids) < 2:
            continue
        for pid in pids:
            pm = product_meta.get(pid)
            if not pm:
                continue
            if _is_x_singleton_product(pm, handle_suffix=handle_suffix, sku_key=sku_key):
                out.add(sku_key)
                break
    return out


def _rows_from_index(
    product_meta: dict[str, dict],
    qualifying: set[str],
    *,
    active_only: bool,
    handle_suffix: str,
) -> list[dict]:
    rows: list[dict] = []
    for pid, pm in product_meta.items():
        st = (pm.get("status") or "").strip().upper()
        if active_only and st != "ACTIVE":
            continue

        match_v: dict | None = None
        match_key: str | None = None
        for v in pm.get("variants") or []:
            sk = v.get("sku_key") or ""
            if sk and sk in qualifying:
                match_v = v
                match_key = sk
                break

        if not match_v or not match_key:
            continue

        if _is_x_singleton_product(pm, handle_suffix=handle_suffix, sku_key=match_key):
            kind = "x_single_variant"
        else:
            kind = "shared_sku_peer"

        rows.append(
            {
                "title": (pm.get("title") or "").strip(),
                "handle": (pm.get("handle") or "").strip(),
                "status": st or "",
                "variant_sku": (match_v.get("sku") or "").strip(),
                "row_kind": kind,
                "product_gid": pid,
                "variant_gid": (match_v.get("variant_gid") or "").strip(),
            }
        )
    return rows


def _run_bulk(
    sess: requests.Session,
    *,
    active_only: bool,
    handle_suffix: str,
) -> list[dict]:
    q = _GQL_BULK_START.replace(
        "BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY)
    )
    body = _graphql_post(sess, q)
    gerrs = body.get("errors")
    if gerrs:
        print("GraphQL-fout bij start bulk export:", file=sys.stderr)
        print(json.dumps(gerrs, indent=2), file=sys.stderr)
        raise SystemExit(2)

    data = body.get("data") or {}
    run = data.get("bulkOperationRunQuery") or {}
    uerr = run.get("userErrors") or []
    if uerr:
        print("Bulk export geweigerd:", file=sys.stderr)
        for e in uerr:
            print(f"  {e.get('field')}: {e.get('message')}", file=sys.stderr)
        raise SystemExit(2)

    bulk = run.get("bulkOperation") or {}
    op_id = bulk.get("id")
    if not op_id:
        print("Geen bulk operation id in response.", file=sys.stderr)
        raise SystemExit(2)

    print(
        "Bulk export gestart (kan enkele minuten duren)...",
        file=sys.stderr,
        flush=True,
    )

    poll_interval = 3.0
    url: str | None = None
    while True:
        poll = _graphql_post(sess, _GQL_POLL, {"id": op_id})
        if poll.get("errors"):
            print("Poll-fout:", poll.get("errors"), file=sys.stderr)
            time.sleep(poll_interval)
            continue

        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        count = node.get("objectCount")
        if count is not None:
            print(
                f"  Status: {status} — objecten verwerkt: {count}",
                file=sys.stderr,
                flush=True,
            )
        else:
            print(f"  Status: {status}", file=sys.stderr, flush=True)

        if status == "COMPLETED":
            url = node.get("url")
            break

        if status in ("FAILED", "CANCELED"):
            err = node.get("errorCode")
            print(f"Bulk export {status.lower()}: {err}", file=sys.stderr)
            raise SystemExit(2)

        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)

    if not url:
        return []

    print("Export downloaden (tijdelijk bestand)...", file=sys.stderr, flush=True)
    tmp_path = _download_bulk_to_temp(sess, url)
    try:
        print("Index opbouwen en duplicaten bepalen...", file=sys.stderr, flush=True)
        product_meta, sku_to_products = _parse_jsonl_build_index(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    qual = _qualifying_skus(
        product_meta, sku_to_products, handle_suffix=handle_suffix
    )
    print(
        f"  Gekwalificeerde SKU’s (x + duplicaat): {len(qual)}",
        file=sys.stderr,
        flush=True,
    )
    return _rows_from_index(
        product_meta,
        qual,
        active_only=active_only,
        handle_suffix=handle_suffix,
    )


def _next_url_from_link_header(link: str | None) -> str | None:
    if not link:
        return None
    for part in link.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().replace("<", "").replace(">", "")
    return None


def _run_rest(
    sess: requests.Session,
    *,
    active_only: bool,
    handle_suffix: str,
) -> list[dict]:
    headers = {"X-Shopify-Access-Token": TOKEN}
    fields = "id,handle,title,status,variants"
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        f"?limit=250&fields={fields}"
    )

    product_meta: dict[str, dict] = {}
    sku_to_products: dict[str, set[str]] = defaultdict(set)

    scanned = 0

    print(
        "Shopify-producten scannen via REST (langzaam bij grote catalogi)...",
        file=sys.stderr,
        flush=True,
    )

    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("Rate limit, wachten...", file=sys.stderr, flush=True)
            time.sleep(2)
            continue
        if r.status_code >= 500:
            print("Shopify serverfout, retry...", file=sys.stderr, flush=True)
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()

        for p in data.get("products", []):
            scanned += 1
            pid = p.get("id")
            if not pid:
                continue
            gid = f"gid://shopify/Product/{pid}"
            handle = (p.get("handle") or "").strip()
            title = (p.get("title") or "").strip()
            st = (p.get("status") or "").strip().upper()
            variants_out: list[dict] = []
            for v in p.get("variants") or []:
                raw_sku = (v.get("sku") or "").strip()
                sk = normalize_sku_key(raw_sku)
                vid = v.get("id")
                vgid = (
                    f"gid://shopify/ProductVariant/{vid}" if vid else ""
                )
                variants_out.append(
                    {"sku": raw_sku, "sku_key": sk, "variant_gid": vgid}
                )
                if sk:
                    sku_to_products[sk].add(gid)
            product_meta[gid] = {
                "handle": handle,
                "title": title,
                "status": st,
                "variants": variants_out,
            }

        print(f"  ... {scanned} producten verwerkt", file=sys.stderr, flush=True)
        url = _next_url_from_link_header(r.headers.get("Link"))
        time.sleep(0.25)

    qual = _qualifying_skus(
        product_meta, sku_to_products, handle_suffix=handle_suffix
    )
    print(
        f"  Gekwalificeerde SKU’s (x + duplicaat): {len(qual)}",
        file=sys.stderr,
        flush=True,
    )
    return _rows_from_index(
        product_meta,
        qual,
        active_only=active_only,
        handle_suffix=handle_suffix,
    )


def _write_csv(rows: list[dict]) -> None:
    admin_base = f"https://{SHOP}/admin/products"
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(
        [
            "row_kind",
            "title",
            "handle",
            "status",
            "variant_sku",
            "product_id_numeric",
            "variant_id_numeric",
            "admin_url",
        ]
    )
    for r in sorted(rows, key=lambda x: (x.get("title") or "").lower()):
        pgid = r.get("product_gid") or ""
        vgid = r.get("variant_gid") or ""
        pid_num = _gid_numeric(pgid) if pgid and "/Product/" in pgid else ""
        vid_num = _gid_numeric(vgid) if vgid and "/ProductVariant/" in vgid else ""
        admin = f"{admin_base}/{pid_num}" if pid_num else ""
        w.writerow(
            [
                r.get("row_kind") or "",
                r.get("title") or "",
                r.get("handle") or "",
                r.get("status") or "",
                r.get("variant_sku") or "",
                pid_num,
                vid_num,
                admin,
            ]
        )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="CSV: x-handle + 1 variant +zelfde SKU elders, plus alle peer-producten."
    )
    ap.add_argument(
        "--rest",
        action="store_true",
        help="REST i.p.v. bulk (langzaam; kleine shops/debug)",
    )
    ap.add_argument(
        "--active-only",
        action="store_true",
        help="Alleen rijen met status ACTIVE",
    )
    ap.add_argument(
        "--handle-suffix",
        default="x",
        metavar="TEXT",
        help="Handle eindigt hierop (case-insensitive); default: x",
    )
    args = ap.parse_args()

    if not TOKEN:
        print(
            "Geen SHOPIFY_ACCESS_TOKEN — zet deze in .env (zie .env.example).",
            file=sys.stderr,
        )
        return 2

    sess = _http_session()
    if args.rest:
        rows = _run_rest(
            sess,
            active_only=args.active_only,
            handle_suffix=args.handle_suffix,
        )
    else:
        rows = _run_bulk(
            sess,
            active_only=args.active_only,
            handle_suffix=args.handle_suffix,
        )

    _write_csv(rows)
    n_x = sum(1 for r in rows if r.get("row_kind") == "x_single_variant")
    n_peer = sum(1 for r in rows if r.get("row_kind") == "shared_sku_peer")
    print(
        f"Rijen: {len(rows)} (x_single_variant: {n_x}, shared_sku_peer: {n_peer}).",
        file=sys.stderr,
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
