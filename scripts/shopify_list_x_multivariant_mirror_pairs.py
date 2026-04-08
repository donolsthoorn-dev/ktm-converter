#!/usr/bin/env python3
"""
CSV (stdout): **ACTIVE** producten die samen een “spiegelpaar” vormen:

- het ene product: **handle eindigt op x** (default), **≥2 varianten**
- het andere: **andere handle**, **dezelfde variant-set** (zelfde opties / titels)
- **beide** alleen als status **ACTIVE** is

“Zelfde varianten” = dezelfde multiset vingerafdruk per variant: GraphQL
`selectedOptions` (naam+waarde), anders REST `option1/2/3`, anders `title`.

Voorbeeld: `3PW24000470x` (meerdere maten) naast `team-pants` met identieke variant-opties.

  python3 scripts/shopify_list_x_multivariant_mirror_pairs.py > output/logs/x_multivariant_mirrors.csv

Optioneel: `--rest` (kleine shops), `--handle-suffix`, `--active-only` weggelaten — alleen ACTIVE
wordt meegenomen (vereiste van de opdracht).

Vereist: `SHOPIFY_ACCESS_TOKEN`, `SHOPIFY_SHOP_DOMAIN` in `.env`.
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

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)
_REQUEST_TIMEOUT_LONG = (12, 600)

# Bulk: varianten met selectedOptions voor betrouwbare vergelijking
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
              selectedOptions {
                name
                value
              }
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation KtmBulkMirrorPairs {
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


def _variant_fingerprint(v: dict) -> tuple:
    opts = v.get("selectedOptions")
    if isinstance(opts, list) and len(opts) > 0:
        pairs = tuple(
            sorted(
                (
                    str(o.get("name") or "").strip().lower(),
                    str(o.get("value") or "").strip().lower(),
                )
                for o in opts
                if isinstance(o, dict)
            )
        )
        return ("opts", pairs)
    o1 = str(v.get("option1") or "").strip().lower()
    o2 = str(v.get("option2") or "").strip().lower()
    o3 = str(v.get("option3") or "").strip().lower()
    if o1 or o2 or o3:
        return ("rest", (o1, o2, o3))
    return ("title", str(v.get("title") or "").strip().lower())


def _product_variant_signature(variants: list[dict]) -> tuple:
    fps = sorted(_variant_fingerprint(v) for v in variants)
    return tuple(fps)


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


def _parse_jsonl_products(path: str) -> dict[str, dict]:
    """product_gid -> {handle, title, status, variants: [raw variant dicts]}"""
    product_meta: dict[str, dict] = {}
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
        product_meta[gid] = {
            "handle": handle,
            "title": title,
            "status": st,
            "variants": list(variant_buf),
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
    return product_meta


def _cluster_matching_pids(
    product_meta: dict[str, dict],
    *,
    handle_suffix: str,
) -> set[str]:
    """
    Product-gids die bij een cluster horen: zelfde variant-signatuur, ≥2 producten,
    minstens één ACTIVE multi-x en minstens één ACTIVE multi-niet-x.
    """
    # Alleen ACTIVE, ≥2 varianten
    sig_to_pids: dict[tuple, list[str]] = defaultdict(list)
    for pid, pm in product_meta.items():
        if (pm.get("status") or "").strip().upper() != "ACTIVE":
            continue
        vs = pm.get("variants") or []
        if len(vs) < 2:
            continue
        sig = _product_variant_signature(vs)
        sig_to_pids[sig].append(pid)

    out: set[str] = set()
    for _sig, pids in sig_to_pids.items():
        if len(pids) < 2:
            continue
        has_x = False
        has_non_x = False
        for pid in pids:
            h = (product_meta[pid].get("handle") or "").strip()
            if _handle_ends_with_suffix(h, handle_suffix):
                has_x = True
            else:
                has_non_x = True
        if has_x and has_non_x:
            out.update(pids)
    return out


def _row_kind(pid: str, product_meta: dict[str, dict], handle_suffix: str) -> str:
    h = (product_meta[pid].get("handle") or "").strip()
    if _handle_ends_with_suffix(h, handle_suffix):
        return "x_multi_variant"
    return "mirror_peer"


def _signature_display(pm: dict) -> str:
    vs = pm.get("variants") or []
    parts: list[str] = []
    for v in vs:
        fp = _variant_fingerprint(v)
        if fp[0] == "opts":
            pairs = fp[1]
            parts.append(
                "|".join(f"{n}={val}" for n, val in pairs) if pairs else "(opties)"
            )
        elif fp[0] == "rest":
            o1, o2, o3 = fp[1]
            parts.append("/".join(x for x in (o1, o2, o3) if x) or "(leeg)")
        else:
            parts.append(fp[1] or "(geen titel)")
    return " || ".join(sorted(parts))


def _run_bulk(sess: requests.Session, *, handle_suffix: str) -> list[dict]:
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

    print("Export downloaden...", file=sys.stderr, flush=True)
    tmp_path = _download_bulk_to_temp(sess, url)
    try:
        print("Parsen en clusters bepalen...", file=sys.stderr, flush=True)
        product_meta = _parse_jsonl_products(tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    keep = _cluster_matching_pids(product_meta, handle_suffix=handle_suffix)
    rows: list[dict] = []
    admin_base = f"https://{SHOP}/admin/products"
    for pid in sorted(keep, key=lambda x: (product_meta[x].get("title") or "").lower()):
        pm = product_meta[pid]
        vs = pm.get("variants") or []
        v0 = vs[0] if vs else {}
        rows.append(
            {
                "row_kind": _row_kind(pid, product_meta, handle_suffix),
                "title": (pm.get("title") or "").strip(),
                "handle": (pm.get("handle") or "").strip(),
                "status": (pm.get("status") or "").strip(),
                "variant_count": len(vs),
                "variant_signature_summary": _signature_display(pm),
                "product_gid": pid,
                "first_variant_gid": (v0.get("id") or "").strip(),
                "admin_url": f"{admin_base}/{_gid_numeric(pid)}",
            }
        )
    return rows


def _next_url_from_link_header(link: str | None) -> str | None:
    if not link:
        return None
    for part in link.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().replace("<", "").replace(">", "")
    return None


def _run_rest(sess: requests.Session, *, handle_suffix: str) -> list[dict]:
    headers = {"X-Shopify-Access-Token": TOKEN}
    fields = "id,handle,title,status,variants"
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        f"?limit=250&fields={fields}"
    )

    product_meta: dict[str, dict] = {}

    print(
        "Shopify-producten scannen via REST (langzaam)...",
        file=sys.stderr,
        flush=True,
    )
    scanned = 0
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
            st = (p.get("status") or "").strip().upper()
            if st != "ACTIVE":
                continue
            handle = (p.get("handle") or "").strip()
            title = (p.get("title") or "").strip()
            variants = []
            for v in p.get("variants") or []:
                variants.append(
                    {
                        "id": f"gid://shopify/ProductVariant/{v.get('id')}",
                        "sku": v.get("sku"),
                        "title": v.get("title"),
                        "option1": v.get("option1"),
                        "option2": v.get("option2"),
                        "option3": v.get("option3"),
                    }
                )
            if len(variants) < 2:
                continue
            product_meta[gid] = {
                "handle": handle,
                "title": title,
                "status": st,
                "variants": variants,
            }

        print(f"  ... {scanned} producten", file=sys.stderr, flush=True)
        url = _next_url_from_link_header(r.headers.get("Link"))
        time.sleep(0.25)

    keep = _cluster_matching_pids(product_meta, handle_suffix=handle_suffix)
    admin_base = f"https://{SHOP}/admin/products"
    rows: list[dict] = []
    for pid in sorted(keep, key=lambda x: (product_meta[x].get("title") or "").lower()):
        pm = product_meta[pid]
        vs = pm.get("variants") or []
        v0 = vs[0] if vs else {}
        rows.append(
            {
                "row_kind": _row_kind(pid, product_meta, handle_suffix),
                "title": (pm.get("title") or "").strip(),
                "handle": (pm.get("handle") or "").strip(),
                "status": (pm.get("status") or "").strip(),
                "variant_count": len(vs),
                "variant_signature_summary": _signature_display(pm),
                "product_gid": pid,
                "first_variant_gid": (v0.get("id") or "").strip(),
                "admin_url": f"{admin_base}/{_gid_numeric(pid)}",
            }
        )
    return rows


def _write_csv(rows: list[dict]) -> None:
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(
        [
            "row_kind",
            "title",
            "handle",
            "status",
            "variant_count",
            "variant_signature_summary",
            "product_id_numeric",
            "first_variant_id_numeric",
            "admin_url",
        ]
    )
    for r in rows:
        pgid = r.get("product_gid") or ""
        vgid = r.get("first_variant_gid") or ""
        w.writerow(
            [
                r.get("row_kind") or "",
                r.get("title") or "",
                r.get("handle") or "",
                r.get("status") or "",
                r.get("variant_count", ""),
                r.get("variant_signature_summary") or "",
                _gid_numeric(pgid) if pgid and "/Product/" in pgid else "",
                _gid_numeric(vgid) if vgid and "/ProductVariant/" in vgid else "",
                r.get("admin_url") or "",
            ]
        )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="ACTIVE x-multi + spiegelproduct metzelfde varianten (CSV naar stdout)."
    )
    ap.add_argument(
        "--rest",
        action="store_true",
        help="REST i.p.v. bulk (langzaam)",
    )
    ap.add_argument(
        "--handle-suffix",
        default="x",
        metavar="TEXT",
        help="Familie-handle eindigt hierop; default: x",
    )
    args = ap.parse_args()

    if not TOKEN:
        print("Geen SHOPIFY_ACCESS_TOKEN in .env.", file=sys.stderr)
        return 2

    sess = _http_session()
    if args.rest:
        rows = _run_rest(sess, handle_suffix=args.handle_suffix)
    else:
        rows = _run_bulk(sess, handle_suffix=args.handle_suffix)

    _write_csv(rows)
    nx = sum(1 for r in rows if r.get("row_kind") == "x_multi_variant")
    np = sum(1 for r in rows if r.get("row_kind") == "mirror_peer")
    print(
        f"Rijen: {len(rows)} (x_multi_variant: {nx}, mirror_peer: {np}).",
        file=sys.stderr,
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
