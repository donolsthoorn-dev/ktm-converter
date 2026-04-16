#!/usr/bin/env python3
"""
Vul ontbrekende Shopify **product types** (Admin: producttype) op basis van de KTM-XML-pipeline
(`load_products`), zonder andere productvelden aan te passen.

1. Haalt alle producten uit Shopify (REST, paginering).
2. Filtert op `product_type` leeg.
3. Zoekt per **handle** de bijbehorende rij in XML-exportlogica; bepaalt het gewenste type
   (zelfde logica als export: bij `config.DELTA_EXCLUDED_TYPES` wordt voorkeur gegeven aan
   **category** als die nuttiger is dan o.a. "Archiv").

Standaard **dry-run**: schrijft alleen een rapport-CSV. Met `--apply` worden mutations uitgevoerd.

  python3 scripts/shopify_fill_missing_product_types.py
  python3 scripts/shopify_fill_missing_product_types.py --apply
  python3 scripts/shopify_fill_missing_product_types.py --limit 20 --apply

Vereist: `SHOPIFY_ACCESS_TOKEN`, `SHOPIFY_SHOP_DOMAIN` in `.env` (via `config`). Niet bruikbaar met
`KTM_SKIP_SHOPIFY_API=1` (live API nodig).
"""

from __future__ import annotations

import argparse
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

import config  # noqa: E402
from modules.xml_loader import load_products, normalize_shopify_product_handle  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (15, 90)

_GQL_UPDATE_TYPE = """
mutation KtmProductType($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      productType
    }
    userErrors {
      field
      message
    }
  }
}
"""


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def resolve_desired_product_type(primary: dict) -> str:
    """
    Zelfde bedoeling als Shopify-kolom Type in exporter: XML-type, met voorkeur voor category
    wanneer het ruwe type in DELTA_EXCLUDED_TYPES zit (bijv. Archiv → PowerWear).
    """
    t = (primary.get("type") or "").strip()
    c = (primary.get("category") or "").strip()
    if t in config.DELTA_EXCLUDED_TYPES:
        return (c or t).strip()
    return (t or c).strip()


def build_xml_primary_by_handle() -> dict[str, dict]:
    """Eén primaire rij per genormaliseerde handle (zelfde keuze als exporter: langste title)."""
    products = load_products()
    by_handle: dict[str, list[dict]] = defaultdict(list)
    for p in products:
        h = normalize_shopify_product_handle(p.get("handle") or "")
        if h:
            by_handle[h].append(p)
    out: dict[str, dict] = {}
    for h, items in by_handle.items():
        out[h] = max(items, key=lambda x: len(x.get("title", "") or ""))
    return out


def fetch_shopify_products_with_types(sess: requests.Session) -> list[dict[str, str]]:
    """Lijst {id, handle, product_type} voor alle producten."""
    rows: list[dict[str, str]] = []
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        "?limit=250&fields=id,handle,product_type"
    )
    headers = {"X-Shopify-Access-Token": TOKEN}
    page = 0
    while url:
        page += 1
        for attempt in range(25):
            r = sess.get(
                url,
                headers=headers,
                timeout=_REQUEST_TIMEOUT,
                proxies={"http": None, "https": None},
            )
            if r.status_code == 429:
                time.sleep(min(2.0 + attempt * 0.3, 45.0))
                continue
            if r.status_code >= 500:
                time.sleep(3.0)
                continue
            r.raise_for_status()
            break
        else:
            print(f"REST products: te veel retries op pagina {page}", file=sys.stderr)
            raise SystemExit(2)

        data = r.json()
        for p in data.get("products") or []:
            pid = p.get("id")
            if pid is None:
                continue
            rows.append(
                {
                    "id": str(int(pid)),
                    "handle": normalize_shopify_product_handle(p.get("handle") or ""),
                    "product_type": (p.get("product_type") or "").strip(),
                }
            )

        print(
            f"Shopify REST pagina {page}: +{len(data.get('products') or [])} — totaal {len(rows)}",
            file=sys.stderr,
            flush=True,
        )

        link = r.headers.get("Link") or ""
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                raw = part.split(";")[0].strip()
                if raw.startswith("<") and raw.endswith(">"):
                    next_url = raw[1:-1]
                else:
                    next_url = raw
                break
        url = next_url
        if url:
            time.sleep(0.5)

    return rows


def _graphql_post(sess: requests.Session, query: str, variables: dict | None) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
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


def product_gid(numeric_id: str) -> str:
    return f"gid://shopify/Product/{numeric_id}"


def update_product_type(sess: requests.Session, numeric_id: str, product_type: str) -> tuple[bool, str]:
    body = _graphql_post(
        sess,
        _GQL_UPDATE_TYPE,
        {
            "input": {
                "id": product_gid(numeric_id),
                "productType": product_type,
            }
        },
    )
    errs = body.get("errors") or []
    if errs:
        return False, json.dumps(errs)[:500]
    upd = (body.get("data") or {}).get("productUpdate") or {}
    user_errors = upd.get("userErrors") or []
    if user_errors:
        return False, json.dumps(user_errors)[:500]
    got = ((upd.get("product") or {}).get("productType") or "").strip()
    if got != product_type.strip():
        return False, f"onverwacht productType terug: {got!r}"
    return True, ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Vul ontbrekende Shopify-producttypes vanuit KTM-XML (dry-run of --apply)."
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Voer productUpdate uit (zonder deze vlag: alleen rapport).",
    )
    ap.add_argument(
        "--output-csv",
        type=Path,
        default=Path("output/shopify_fill_missing_product_types.csv"),
        metavar="PAD",
        help="Rapport-CSV (default: output/shopify_fill_missing_product_types.csv)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Maximaal N updates bij --apply (0 = geen limiet).",
    )
    args = ap.parse_args()

    if os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in ("1", "true", "yes"):
        print("KTM_SKIP_SHOPIFY_API staat aan; dit script heeft live Shopify nodig.", file=sys.stderr)
        return 2

    if not TOKEN or not SHOP:
        print("SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN zijn verplicht (.env).", file=sys.stderr)
        return 2

    print("XML laden (kan even duren)...", file=sys.stderr, flush=True)
    xml_by_handle = build_xml_primary_by_handle()
    print(f"XML-index: {len(xml_by_handle)} handles.", file=sys.stderr, flush=True)

    sess = _http_session()
    shop_rows = fetch_shopify_products_with_types(sess)
    missing = [r for r in shop_rows if not (r.get("product_type") or "").strip()]

    report_rows: list[list[str]] = []
    to_apply: list[tuple[str, str, str, str]] = []  # id, handle, nieuw_type, reden

    for r in missing:
        hid = r["id"]
        handle = r["handle"]
        primary = xml_by_handle.get(handle)
        if not primary:
            report_rows.append([hid, handle, "", "", "geen XML-match op handle"])
            continue
        desired = resolve_desired_product_type(primary)
        if not desired:
            report_rows.append(
                [hid, handle, "", (primary.get("type") or ""), "XML-type en -category leeg"]
            )
            continue
        report_rows.append(
            [
                hid,
                handle,
                desired,
                (primary.get("type") or ""),
                "klaar voor update" if args.apply else "dry-run",
            ]
        )
        to_apply.append((hid, handle, desired, (primary.get("type") or "")))

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(
            [
                "Product_id",
                "Handle",
                "Nieuw_product_type",
                "Xml_type_ruw",
                "Toelichting",
            ]
        )
        w.writerows(report_rows)

    n_missing = len(missing)
    n_xml_hit = sum(1 for row in report_rows if row[2])
    n_no_xml = sum(1 for row in report_rows if row[4] == "geen XML-match op handle")
    n_empty_desired = sum(1 for row in report_rows if "leeg" in row[4])

    print(
        f"Samenvatting: Shopify zonder type: {n_missing}; "
        f"met voorstel uit XML: {n_xml_hit}; "
        f"geen handle in XML: {n_no_xml}; "
        f"XML zonder type/category: {n_empty_desired}.",
        flush=True,
    )
    print(f"Rapport: {args.output_csv}", flush=True)

    if not args.apply:
        print("Dry-run (geen API-updates). Voeg --apply toe om bij te werken.", flush=True)
        return 0

    limit = max(0, int(args.limit or 0))
    done = 0
    ok = 0
    failed: list[str] = []

    for idx, (pid, handle, desired, _xml_t) in enumerate(to_apply, start=1):
        if limit and done >= limit:
            print(f"Gestopt na --limit {limit}.", flush=True)
            break
        good, err = update_product_type(sess, pid, desired)
        done += 1
        if good:
            ok += 1
            print(f"  [{idx}/{len(to_apply)}] OK {handle} → {desired!r} ({pid})", flush=True)
        else:
            failed.append(f"{handle} ({pid}): {err}")
            print(f"  [{idx}/{len(to_apply)}] FOUT {handle}: {err}", file=sys.stderr, flush=True)
        time.sleep(0.2)

    print(f"Updates gelukt: {ok}/{done}. Mislukt: {len(failed)}.", flush=True)
    if failed:
        for line in failed[:25]:
            print(line, file=sys.stderr)
        if len(failed) > 25:
            print(f"... en {len(failed) - 25} meer.", file=sys.stderr)
        return 1 if ok == 0 else 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
