#!/usr/bin/env python3
"""
Bouw en vul Supabase lookup-tabel met Shopify type -> HS-code + HS-omschrijving.

Doel:
- Snel kunnen zien welke Shopify producttypes nu op welke HS-code uitkomen.
- Per HS-code een omschrijving opslaan op basis van tariffnumber.com (cnSuggest API).

Input:
- SKU-level mapping CSV (default: input/customs_mapping_types_batch1.csv)

Output:
- Upsert in public.customs_type_hs_mapping

Gebruik:
  python3 scripts/build_customs_type_hs_supabase_mapping.py --dry-run
  python3 scripts/build_customs_type_hs_supabase_mapping.py
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

_REQUEST_TIMEOUT = (30, 120)
_PAGE = 1000


def _rest_base() -> str:
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    if not url:
        raise SystemExit("SUPABASE_URL ontbreekt")
    return f"{url}/rest/v1"


def _headers(*, upsert: bool = False) -> dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        raise SystemExit("SUPABASE_SERVICE_ROLE_KEY ontbreekt")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if upsert:
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return headers


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
        r.raise_for_status()
        rows = r.json() or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < _PAGE:
            break
        offset += _PAGE
    return out


def _load_sku_hs_map(path: Path) -> dict[str, dict[str, str]]:
    if not path.is_file():
        raise SystemExit(f"CSV niet gevonden: {path}")
    out: dict[str, dict[str, str]] = {}
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            sku = str(row.get("sku") or "").strip().upper()
            hs = re.sub(r"\D+", "", str(row.get("hs_code") or ""))
            source = str(row.get("source") or "").strip()
            if not sku or not hs:
                continue
            out[sku] = {"hs_code": hs, "source": source}
    return out


def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]*>", "", s or "")


def _clean_tariff_label(value: str, code: str) -> str:
    txt = _strip_html(value)
    txt = re.sub(r"\s+", " ", txt).strip()
    if txt.startswith(code):
        txt = txt[len(code) :].strip(" -")
    return txt


def _fetch_hs_description(
    sess: requests.Session,
    hs_code: str,
    *,
    year: int,
    lang: str,
) -> tuple[str, str]:
    url = "https://www.tariffnumber.com/api/v1/cnSuggest"
    r = sess.get(
        url,
        params={"term": hs_code, "lang": lang, "year": str(year)},
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    body = r.json() or {}
    suggestions = body.get("suggestions") or []
    if not suggestions:
        return "", ""
    exact = None
    for item in suggestions:
        if str(item.get("code") or "").strip() == hs_code:
            exact = item
            break
    chosen = exact or suggestions[0]
    desc = _clean_tariff_label(str(chosen.get("value") or ""), hs_code)
    src_url = str(chosen.get("data") or "").strip()
    return desc, src_url


def _reset_table(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    *,
    dry_run: bool,
) -> None:
    if dry_run:
        print("[dry-run] customs_type_hs_mapping zou geleegd worden.", flush=True)
        return
    r = sess.delete(
        f"{base}/customs_type_hs_mapping",
        headers=headers,
        params={"product_type": "not.is.null"},
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    print("customs_type_hs_mapping geleegd (full rebuild).", flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--mapping-csv",
        default=str(ROOT / "input" / "customs_mapping_types_batch1.csv"),
        help="SKU-level customs mapping CSV (sku;hs_code;country_of_origin;source)",
    )
    p.add_argument(
        "--tariff-year",
        type=int,
        default=2026,
        help="Jaar voor tariffnumber lookup (default: 2026)",
    )
    p.add_argument(
        "--tariff-lang",
        default="en",
        choices=("en", "de", "fr"),
        help="Taal voor tariffnumber lookup (default: en)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Geen writes naar Supabase; wel berekening en voorbeeldoutput",
    )
    args = p.parse_args()

    base = _rest_base()
    headers_ro = _headers(upsert=False)
    headers_upsert = _headers(upsert=True)
    sess = requests.Session()
    sess.trust_env = False

    sku_map = _load_sku_hs_map(Path(args.mapping_csv))
    print(f"CSV mapping geladen: {len(sku_map)} SKU's", flush=True)

    print("Supabase: shopify_products ophalen...", flush=True)
    products = _fetch_paginated(
        sess,
        base,
        headers_ro,
        "shopify_products",
        "shopify_product_id,type",
    )
    type_by_pid: dict[int, str] = {}
    for row in products:
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        t = str(row.get("type") or "").strip()
        if not t:
            continue
        type_by_pid[int(pid)] = t

    print("Supabase: shopify_variants ophalen...", flush=True)
    variants = _fetch_paginated(
        sess,
        base,
        headers_ro,
        "shopify_variants",
        "shopify_variant_id,shopify_product_id,sku",
    )
    print(f"Mirror varianten: {len(variants)}", flush=True)

    sku_count_by_type: Counter[str] = Counter()
    mapped_sku_count_by_type: Counter[str] = Counter()
    hs_count_by_type: dict[str, Counter[str]] = defaultdict(Counter)
    source_count_by_type_hs: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)

    for row in variants:
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        product_type = type_by_pid.get(int(pid), "")
        if not product_type:
            continue
        sku = str(row.get("sku") or "").strip().upper()
        if not sku:
            continue

        sku_count_by_type[product_type] += 1
        mapped = sku_map.get(sku) or {}
        hs = str(mapped.get("hs_code") or "").strip()
        if not hs:
            continue

        mapped_sku_count_by_type[product_type] += 1
        hs_count_by_type[product_type][hs] += 1
        src = str(mapped.get("source") or "").strip()
        if src:
            source_count_by_type_hs[(product_type, hs)][src] += 1

    unique_hs_codes = sorted(
        {
            hs
            for type_name, hs_counter in hs_count_by_type.items()
            if sku_count_by_type.get(type_name, 0) > 0
            for hs in hs_counter.keys()
        }
    )
    hs_desc_cache: dict[str, tuple[str, str]] = {}
    for hs in unique_hs_codes:
        try:
            hs_desc_cache[hs] = _fetch_hs_description(
                sess,
                hs,
                year=args.tariff_year,
                lang=args.tariff_lang,
            )
        except requests.RequestException as exc:
            print(f"Waarschuwing: tariffnumber lookup faalde voor {hs}: {exc}", flush=True)
            hs_desc_cache[hs] = ("", "")
        time.sleep(0.1)

    rows_out: list[dict[str, Any]] = []
    for product_type in sorted(sku_count_by_type.keys()):
        hs_counter = hs_count_by_type.get(product_type) or Counter()
        chosen_hs = ""
        mapping_source = ""
        if hs_counter:
            chosen_hs = sorted(hs_counter.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
            src_counter = source_count_by_type_hs.get((product_type, chosen_hs)) or Counter()
            if src_counter:
                mapping_source = sorted(src_counter.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        hs_description, hs_url = hs_desc_cache.get(chosen_hs, ("", ""))

        rows_out.append(
            {
                "product_type": product_type,
                "hs_code": chosen_hs or None,
                "hs_description": hs_description or None,
                "hs_description_source": "tariffnumber.com cnSuggest"
                if chosen_hs
                else None,
                "hs_description_url": hs_url or None,
                "mapping_source": mapping_source or None,
                "sku_count": int(sku_count_by_type[product_type]),
                "mapped_sku_count": int(mapped_sku_count_by_type[product_type]),
                "tariff_year": int(args.tariff_year) if chosen_hs else None,
                "tariff_lang": args.tariff_lang if chosen_hs else None,
                "updated_at": "now()",
            }
        )

    mapped_types = sum(1 for r in rows_out if r["hs_code"])
    print(
        f"Types totaal: {len(rows_out)} | met HS: {mapped_types} | zonder HS: {len(rows_out) - mapped_types}",
        flush=True,
    )

    if args.dry_run:
        for row in rows_out[:25]:
            print(json.dumps(row, ensure_ascii=False), flush=True)
        if len(rows_out) > 25:
            print(f"... en {len(rows_out) - 25} rijen meer", flush=True)
        return 0

    _reset_table(sess, base, headers_ro, dry_run=args.dry_run)

    clean_rows: list[dict[str, Any]] = []
    for row in rows_out:
        row_copy = dict(row)
        # updated_at laten we door Postgres invullen.
        row_copy.pop("updated_at", None)
        clean_rows.append(row_copy)

    url = f"{base}/customs_type_hs_mapping"
    chunk_size = 400
    for i in range(0, len(clean_rows), chunk_size):
        chunk = clean_rows[i : i + chunk_size]
        r = sess.post(
            url,
            headers=headers_upsert,
            params={"on_conflict": "product_type"},
            data=json.dumps(chunk),
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        print(f"Upsert {min(i + chunk_size, len(clean_rows))}/{len(clean_rows)}", flush=True)

    print("Klaar: public.customs_type_hs_mapping gevuld.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
