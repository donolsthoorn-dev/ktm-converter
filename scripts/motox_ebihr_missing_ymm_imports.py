#!/usr/bin/env python3
"""
Motox: live producten zonder YMM-metafields, aangevuld uit e-bihr CSV's.

Maakt YMM-app + Metafields Manager importbestanden met echte Shopify product-IDs.

  python3 scripts/motox_ebihr_missing_ymm_imports.py

Laadt `.env.motox` (overschrijft SHOPIFY_*).
"""

from __future__ import annotations

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

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env.motox", override=True)

import config  # noqa: E402 — Motox SHOPIFY_* al in environ
from modules.metafields_manager_export import (  # noqa: E402
    METAFIELDS_HEADER,
    _pipe_join_sorted,
    _ymm_summary,
    _ymm_tuples_to_fits_on_json,
)
from modules.ymm_export import (  # noqa: E402
    YMM_MAX_FILE_SIZE_BYTES,
    split_csv_max_bytes_with_header,
)

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_TIMEOUT = (12, 600)

EBIHR = ROOT / "motox" / "e-bihr"
OUT_DIR = EBIHR / "shopify_import_ready"

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        fitsOn: metafield(namespace: "global", key: "fits_on") { value }
        ymmSummary: metafield(namespace: "global", key: "ymm_summary") { value }
        variants {
          edges {
            node { sku }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation MotoxEbihrYmmBulk {
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
query MotoxPollBulk($id: ID!) {
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


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, query: str, variables: dict | None = None) -> dict:
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
            timeout=_TIMEOUT,
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


def _gid_num(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def _wait_current_bulk(sess: requests.Session) -> None:
    while True:
        body = _gql(sess, _GQL_CURRENT)
        cur = ((body.get("data") or {}).get("currentBulkOperation")) or {}
        status = (cur.get("status") or "").upper()
        if not status or status in ("COMPLETED", "FAILED", "CANCELED"):
            return
        print(f"  Wacht op lopende bulk: {status} ({cur.get('objectCount')})", flush=True)
        time.sleep(5)


def _run_bulk(sess: requests.Session) -> Path:
    _wait_current_bulk(sess)
    q = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY))
    body = _gql(sess, q)
    if body.get("errors"):
        raise SystemExit(f"GraphQL start-fout: {body.get('errors')}")
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    uerr = run.get("userErrors") or []
    if uerr:
        raise SystemExit(f"Bulk geweigerd: {uerr}")
    op_id = (run.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise SystemExit("Geen bulk operation id.")
    print("Bulk export gestart...", flush=True)
    poll_interval = 3.0
    while True:
        poll = _gql(sess, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            url = node.get("url")
            if not url:
                raise SystemExit("Bulk klaar zonder URL.")
            dest = OUT_DIR / "shopify_products_bulk.jsonl"
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            with sess.get(
                url, stream=True, timeout=_TIMEOUT, proxies={"http": None, "https": None}
            ) as r:
                r.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in r.iter_content(1024 * 256):
                        if chunk:
                            f.write(chunk)
            return dest
        if status in ("FAILED", "CANCELED"):
            raise SystemExit(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)


def _parse_bulk(path: Path) -> dict[str, dict]:
    """product_gid -> {id, handle, title, status, fits_on, ymm_summary, skus}"""
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
                products[gid] = {
                    "id": _gid_num(gid),
                    "handle": (obj.get("handle") or "").strip(),
                    "title": (obj.get("title") or "").strip(),
                    "status": (obj.get("status") or "").strip().upper(),
                    "fits_on": ((obj.get("fitsOn") or {}) or {}).get("value") or "",
                    "ymm_summary": ((obj.get("ymmSummary") or {}) or {}).get("value") or "",
                    "skus": [],
                }
            elif gid.startswith("gid://shopify/ProductVariant/") and parent:
                sku = (obj.get("sku") or "").strip()
                if sku and parent in products:
                    products[parent]["skus"].append(sku)
    return products


def _has_ymm(p: dict) -> bool:
    return bool((p.get("fits_on") or "").strip() or (p.get("ymm_summary") or "").strip())


def _json_to_tuples(raw: str) -> set[tuple[str, str, str]]:
    out: set[tuple[str, str, str]] = set()
    raw = (raw or "").strip()
    if not raw:
        return out
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return out
    if not isinstance(data, dict):
        return out
    for make, models in data.items():
        if not isinstance(models, dict):
            continue
        for model, years in models.items():
            if isinstance(years, dict):
                years = years.keys()
            if not isinstance(years, (list, tuple, set)):
                continue
            for y in years:
                ys = str(y).strip()
                if make and model and ys:
                    out.add((str(make).strip(), str(model).strip(), ys))
    return out


def _load_ebihr_metafields() -> dict[str, str]:
    """handle -> fits_on JSON"""
    out: dict[str, str] = {}
    d = EBIHR / "ymm-metafields-data-ebihr-1789470542_chunks"
    for p in sorted(d.glob("*.csv")):
        with p.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                h = (row.get("handle") or "").strip()
                fo = (row.get("fits_on") or "").strip()
                if h and fo:
                    out[h] = fo
    return out


def _load_ebihr_filter() -> dict[str, set[tuple[str, str, str]]]:
    """sku/handle token -> {(make, model, year)}"""
    out: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    paths = [EBIHR / "ymm-filter-data-ebihr-1789470542.csv"]
    parts = EBIHR / "ymm-filter-data-ebihr-parts"
    if parts.is_dir():
        paths = sorted(parts.glob("*.csv"))
    for p in paths:
        with p.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                make = (row.get("Make") or "").strip()
                model = (row.get("Model") or "").strip()
                year = (row.get("Year") or "").strip()
                ids = row.get("Product Ids") or ""
                if not (make and model and year and ids):
                    continue
                for tok in ids.split("~"):
                    t = tok.strip()
                    if t:
                        out[t].add((make, model, year))
    return out


def _tuples_for_product(
    p: dict,
    mf: dict[str, str],
    filt: dict[str, set[tuple[str, str, str]]],
) -> set[tuple[str, str, str]]:
    keys = [p["handle"], *p["skus"]]
    for k in keys:
        raw = mf.get(k)
        if raw:
            t = _json_to_tuples(raw)
            if t:
                return t
    union: set[tuple[str, str, str]] = set()
    for k in keys:
        union |= filt.get(k, set())
    return union


def _write_split_csv(path: Path, header: list[str], rows: list[list[str]]) -> list[Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    if path.stat().st_size <= YMM_MAX_FILE_SIZE_BYTES:
        return [path]
    parts = split_csv_max_bytes_with_header(str(path), YMM_MAX_FILE_SIZE_BYTES)
    return [Path(x) for x in parts]


def main() -> int:
    if not TOKEN or not SHOP:
        print("SHOPIFY credentials ontbreken in .env.motox", file=sys.stderr)
        return 2
    print(f"Shop: {SHOP}", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    sess = _session()
    jsonl = _run_bulk(sess)
    products = _parse_bulk(jsonl)
    active = [p for p in products.values() if p["status"] == "ACTIVE"]
    missing = [p for p in active if not _has_ymm(p)]
    print(
        f"Live ACTIVE: {len(active)}; zonder global.fits_on/ymm_summary: {len(missing)}",
        flush=True,
    )

    print("e-bihr bestanden laden...", flush=True)
    mf = _load_ebihr_metafields()
    filt = _load_ebihr_filter()
    print(f"  metafields handles: {len(mf)}; filter tokens: {len(filt)}", flush=True)

    ymm_rows: list[list[str]] = []
    meta_rows: list[list[str]] = []
    report_rows: list[dict] = []
    matched = 0
    unmatched = 0

    for p in sorted(missing, key=lambda x: x["handle"]):
        tuples = _tuples_for_product(p, mf, filt)
        if not tuples:
            unmatched += 1
            report_rows.append(
                {
                    "shopify_id": p["id"],
                    "handle": p["handle"],
                    "title": p["title"],
                    "status": "geen_ebihr_ymm",
                    "ymm_tuples": 0,
                }
            )
            continue
        matched += 1
        pid = p["id"]
        handle = p["handle"]
        title = p["title"]
        fits_on = _ymm_tuples_to_fits_on_json(tuples)
        years = {t[2] for t in tuples}
        makes = {t[0] for t in tuples}
        models = {t[1] for t in tuples}
        summary = _ymm_summary(tuples)
        mpn = (p["skus"][0] if p["skus"] else handle).upper()
        for make, model, year in sorted(tuples, key=lambda t: (t[0], t[1], t[2])):
            ymm_rows.append([pid, make, model, year])
        meta = {k: "" for k in METAFIELDS_HEADER}
        meta.update(
            {
                "id": pid,
                "handle": handle,
                "title": title,
                "fits_on": fits_on,
                "fits_on_year": _pipe_join_sorted(years),
                "fits_on_make": _pipe_join_sorted(makes),
                "fits_on_model": _pipe_join_sorted(models),
                "ymm_summary": summary,
                "MPN": mpn,
            }
        )
        meta_rows.append([meta[k] for k in METAFIELDS_HEADER])
        report_rows.append(
            {
                "shopify_id": pid,
                "handle": handle,
                "title": title,
                "status": "import_klaar",
                "ymm_tuples": len(tuples),
            }
        )

    ymm_paths = _write_split_csv(
        OUT_DIR / "ymm_APP_import_ebihr_missing.csv",
        ["Product Ids", "Make", "Model", "Year"],
        ymm_rows,
    )
    meta_paths = _write_split_csv(
        OUT_DIR / "product_metafields_ebihr_missing.csv",
        METAFIELDS_HEADER,
        meta_rows,
    )
    report_path = OUT_DIR / "report_live_zonder_ymm.csv"
    with report_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["shopify_id", "handle", "title", "status", "ymm_tuples"],
            lineterminator="\n",
        )
        w.writeheader()
        w.writerows(report_rows)

    print()
    print(f"Gevonden in e-bihr: {matched} producten → importbestanden")
    print(f"Live zonder YMM, niet in e-bihr-bestanden: {unmatched}")
    print("YMM-app:")
    for pth in ymm_paths:
        print(f"  {pth}  ({pth.stat().st_size / 1024 / 1024:.2f} MB)")
    print("Metafields Manager:")
    for pth in meta_paths:
        print(f"  {pth}  ({pth.stat().st_size / 1024 / 1024:.2f} MB)")
    print(f"Rapport: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
