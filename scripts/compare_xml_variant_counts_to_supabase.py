#!/usr/bin/env python3
"""
Tel varianten (unieke SKU's) per export-handle in de KTM-XML en vergelijk met de
Supabase-spiegel (shopify_products + shopify_variants).

Credentials: ``modules.env_loader.load_project_env()`` laadt ``.env``,
``converter/.env``, ``converter/.env.local`` en zet ``SUPABASE_URL`` van
``NEXT_PUBLIC_SUPABASE_URL`` indien nodig. Daarnaast: ``SUPABASE_SERVICE_ROLE_KEY``.

XML-pad: zie config (KTM_XML_FILE / nieuwste CBEXPDN_KTM-DN*.xml in input/).

Rapport: handles waar XML méér unieke SKU's heeft dan er variant-rijen in de mirror staan.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

from modules.xml_loader import load_products, normalize_shopify_product_handle  # noqa: E402

_REQUEST_TIMEOUT = (30, 120)
_PAGE = 1000


def _rest_base() -> str:
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    if not url:
        print("SUPABASE_URL ontbreekt", file=sys.stderr)
        raise SystemExit(1)
    return f"{url}/rest/v1"


def _headers() -> dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        print("SUPABASE_SERVICE_ROLE_KEY ontbreekt", file=sys.stderr)
        raise SystemExit(1)
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _fetch_paginated(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    table: str,
    select: str,
    order: str | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        params: dict[str, str] = {
            "select": select,
            "limit": str(_PAGE),
            "offset": str(offset),
        }
        if order:
            params["order"] = order
        r = sess.get(
            f"{base}/{table}",
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            body = (e.response.text or "")[:2500]
            print(
                f"Supabase GET {table} → HTTP {e.response.status_code}: {body}",
                file=sys.stderr,
                flush=True,
            )
            raise SystemExit(1) from e
        chunk = r.json()
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < _PAGE:
            break
        offset += _PAGE
    return out


def _aggregate_xml() -> tuple[dict[str, set[str]], dict[str, str]]:
    rows = load_products()
    skus_by_handle: dict[str, set[str]] = defaultdict(set)
    title_by_handle: dict[str, str] = {}
    for r in rows:
        raw_h = r.get("handle") or ""
        h = normalize_shopify_product_handle(str(raw_h))
        if not h:
            continue
        sku = (r.get("sku") or "").strip()
        if sku:
            skus_by_handle[h].add(sku)
        t = (r.get("title") or "").strip()
        if t and h not in title_by_handle:
            title_by_handle[h] = t
    return skus_by_handle, title_by_handle


def main() -> int:
    p = argparse.ArgumentParser(
        description="XML-variantaantallen vs Supabase shopify_variants mirror (per handle)."
    )
    p.add_argument(
        "--output-csv",
        default=str(ROOT / "output" / "xml_vs_shopify_variant_counts.csv"),
        help="CSV met alleen handles waar xml_count > shopify_count",
    )
    p.add_argument(
        "--also-missing-in-shopify",
        action="store_true",
        help="Voeg rijen toe: handle in XML maar geen product in mirror (shopify_count=0).",
    )
    args = p.parse_args()

    print("XML laden en per handle unieke SKU's tellen…", flush=True)
    xml_skus_by_handle, xml_title_by_handle = _aggregate_xml()
    n_xml_handles = len(xml_skus_by_handle)
    n_xml_skus = sum(len(s) for s in xml_skus_by_handle.values())
    print(f"  → {n_xml_handles} handles, {n_xml_skus} unieke SKU–handle koppelingen", flush=True)

    base = _rest_base()
    headers = _headers()
    sess = requests.Session()
    sess.trust_env = False

    print("Supabase: shopify_products…", flush=True)
    products = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_products",
        "shopify_product_id,handle,title",
        order="shopify_product_id.asc",
    )
    pid_by_handle: dict[int, str] = {}
    handle_by_pid: dict[int, str] = {}
    title_shop_by_pid: dict[int, str] = {}
    dup_handles: list[str] = []
    for row in products:
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        ipid = int(pid)
        h = normalize_shopify_product_handle(str(row.get("handle") or ""))
        if not h:
            continue
        if h in pid_by_handle and pid_by_handle[h] != ipid:
            dup_handles.append(h)
        pid_by_handle[h] = ipid
        handle_by_pid[ipid] = h
        ts = (row.get("title") or "").strip()
        if ts:
            title_shop_by_pid[ipid] = ts

    print("Supabase: shopify_variants…", flush=True)
    variants = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_variants",
        "shopify_variant_id,shopify_product_id,sku",
        order="shopify_variant_id.asc",
    )
    variant_rows_by_pid: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for v in variants:
        pid = v.get("shopify_product_id")
        if pid is None:
            continue
        variant_rows_by_pid[int(pid)].append(v)

    report_rows: list[dict[str, Any]] = []
    only_xml = 0
    xml_gt_shopify = 0

    for h in sorted(xml_skus_by_handle.keys()):
        xml_n = len(xml_skus_by_handle[h])
        pid = pid_by_handle.get(h)
        if pid is None:
            only_xml += 1
            if args.also_missing_in_shopify:
                report_rows.append(
                    {
                        "handle": h,
                        "xml_variant_count": xml_n,
                        "shopify_variant_count": 0,
                        "shopify_product_id": "",
                        "delta": xml_n,
                        "title_xml": xml_title_by_handle.get(h, ""),
                        "title_shopify": "",
                        "skus_in_xml_not_in_mirror": "",
                        "note": "no_shopify_product_in_mirror",
                    }
                )
            continue

        vrows = variant_rows_by_pid.get(pid, [])
        shopify_n = len(vrows)
        if xml_n > shopify_n:
            xml_gt_shopify += 1
            skus_mirror = sorted(
                {(vr.get("sku") or "").strip().upper() for vr in vrows if (vr.get("sku") or "").strip()}
            )
            skus_xml = sorted(s.upper() for s in xml_skus_by_handle[h])
            in_xml_not_mirror = sorted(set(skus_xml) - set(skus_mirror))
            report_rows.append(
                {
                    "handle": h,
                    "xml_variant_count": xml_n,
                    "shopify_variant_count": shopify_n,
                    "shopify_product_id": str(pid),
                    "delta": xml_n - shopify_n,
                    "title_xml": xml_title_by_handle.get(h, ""),
                    "title_shopify": title_shop_by_pid.get(pid, ""),
                    "skus_in_xml_not_in_mirror": ";".join(in_xml_not_mirror[:50])
                    + (";…" if len(in_xml_not_mirror) > 50 else ""),
                    "note": "",
                }
            )

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "handle",
        "xml_variant_count",
        "shopify_variant_count",
        "shopify_product_id",
        "delta",
        "title_xml",
        "title_shopify",
        "skus_in_xml_not_in_mirror",
        "note",
    ]
    with open(out_path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in report_rows:
            w.writerow(row)

    print(
        json.dumps(
            {
                "xml_handles": n_xml_handles,
                "shopify_products_in_mirror": len(products),
                "xml_gt_shopify_variant_rows": xml_gt_shopify,
                "xml_handles_without_mirror_product": only_xml,
                "duplicate_handles_colliding_pid": len(set(dup_handles)),
                "csv": str(out_path),
            },
            indent=2,
        ),
        flush=True,
    )
    if dup_handles:
        print(
            f"Waarschuwing: {len(set(dup_handles))} handle(s) wijzen naar meer dan één product_id "
            f"(laatste wins); eerste voorbeelden: {sorted(set(dup_handles))[:5]}",
            file=sys.stderr,
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
