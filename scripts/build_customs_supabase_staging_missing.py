#!/usr/bin/env python3
"""
Bouw customs-staging voor varianten met missende customs-data (HS/COO).

Doel:
- Dagelijkse "aanvul-run" voor Shopify-varianten die nog geen HS-code en/of
  country of origin hebben.
- HS-code komt uit Supabase lookup `public.customs_type_hs_mapping` op basis van
  Shopify `product_type`.
- COO wordt gezet naar een vaste default (standaard `AT`) als die ontbreekt.
- Alleen rows met echte customs-wijziging worden in `pricelist_sync_staging`
  geschreven met `customs_changed=true`.

Gebruik:
  python3 scripts/build_customs_supabase_staging_missing.py --dry-run
  python3 scripts/build_customs_supabase_staging_missing.py --batch-id <uuid>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.customs_mapping import (  # noqa: E402
    normalize_country_code,
    normalize_hs_code,
    parse_allowed_hs_lengths,
)
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
    out = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if upsert:
        out["Prefer"] = "return=minimal"
    return out


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


def _customs_changed(
    mirror_hs: str | None,
    mirror_country: str | None,
    proposed_hs: str | None,
    proposed_country: str | None,
) -> bool:
    if proposed_hs and (mirror_hs or "") != proposed_hs:
        return True
    if proposed_country and (mirror_country or "") != proposed_country:
        return True
    return False


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--batch-id",
        type=uuid.UUID,
        default=None,
        help="Optioneel vaste batch UUID (default: nieuwe uuid4)",
    )
    p.add_argument(
        "--country",
        default="AT",
        help="COO default voor missende COO (default: AT)",
    )
    p.add_argument(
        "--missing-mode",
        choices=("both", "any"),
        default="both",
        help="'both' = alleen varianten waar HS en COO ontbreken; 'any' = een van beide ontbreekt",
    )
    p.add_argument(
        "--allowed-hs-lengths",
        default=os.environ.get("SHOPIFY_CUSTOMS_ALLOWED_HS_LENGTHS", "6,8,10"),
        help="Toegestane HS-code lengtes na normalisatie (default: 6,8,10)",
    )
    p.add_argument(
        "--review-csv",
        default=str(ROOT / "output" / "customs_missing_review.csv"),
        help="CSV rapport met overgeslagen/ongemapte varianten",
    )
    p.add_argument("--dry-run", action="store_true", help="Geen insert naar Supabase staging")
    args = p.parse_args()

    allowed_hs_lengths = parse_allowed_hs_lengths(args.allowed_hs_lengths)
    forced_country = normalize_country_code(args.country)
    if not forced_country:
        raise SystemExit(f"Ongeldige country code: {args.country!r}")
    batch_id = args.batch_id or uuid.uuid4()

    base = _rest_base()
    headers_ro = _headers(upsert=False)
    headers_wr = _headers(upsert=True)
    sess = requests.Session()
    sess.trust_env = False

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
        type_by_pid[int(pid)] = str(row.get("type") or "").strip()

    print("Supabase: customs_type_hs_mapping ophalen...", flush=True)
    type_map_rows = _fetch_paginated(
        sess,
        base,
        headers_ro,
        "customs_type_hs_mapping",
        "product_type,hs_code,mapping_source",
    )
    hs_by_type: dict[str, tuple[str, str]] = {}
    for row in type_map_rows:
        t = str(row.get("product_type") or "").strip()
        hs = normalize_hs_code(row.get("hs_code"), allowed_hs_lengths)
        if not t or not hs:
            continue
        src = str(row.get("mapping_source") or "").strip() or "customs_type_hs_mapping"
        hs_by_type[t] = (hs, src)

    print("Supabase: shopify_variants ophalen...", flush=True)
    variants = _fetch_paginated(
        sess,
        base,
        headers_ro,
        "shopify_variants",
        (
            "shopify_variant_id,shopify_product_id,sku,inventory_item_id,"
            "harmonized_system_code,country_code_of_origin"
        ),
        order="shopify_variant_id.asc",
    )
    print(f"Mirror varianten: {len(variants)}", flush=True)

    rows_out: list[dict[str, Any]] = []
    review_rows: list[dict[str, str]] = []
    seen_missing = 0

    for row in variants:
        vid_raw = row.get("shopify_variant_id")
        pid_raw = row.get("shopify_product_id")
        inv_item_raw = row.get("inventory_item_id")
        if vid_raw is None or pid_raw is None:
            continue
        vid = int(vid_raw)
        pid = int(pid_raw)
        inv_item = int(inv_item_raw) if inv_item_raw is not None else None
        sku = str(row.get("sku") or "").strip().upper()

        mirror_hs = normalize_hs_code(row.get("harmonized_system_code"), allowed_hs_lengths)
        mirror_country = normalize_country_code(row.get("country_code_of_origin"))
        missing_hs = not bool(mirror_hs)
        missing_country = not bool(mirror_country)

        if args.missing_mode == "both":
            if not (missing_hs and missing_country):
                continue
        else:
            if not (missing_hs or missing_country):
                continue

        seen_missing += 1

        if inv_item is None:
            review_rows.append(
                {
                    "shopify_variant_id": str(vid),
                    "sku": sku,
                    "product_type": str(type_by_pid.get(pid, "")),
                    "reason": "missing_inventory_item_id",
                }
            )
            continue

        product_type = str(type_by_pid.get(pid, "") or "").strip()
        mapped_hs, mapped_source = hs_by_type.get(product_type, ("", ""))

        proposed_hs = mapped_hs if missing_hs else None
        proposed_country = forced_country if missing_country else None
        if not proposed_hs and not proposed_country:
            continue

        if not proposed_hs and missing_hs:
            review_rows.append(
                {
                    "shopify_variant_id": str(vid),
                    "sku": sku,
                    "product_type": product_type,
                    "reason": "missing_type_hs_mapping",
                }
            )
            # COO-only update nog steeds uitvoeren als COO ontbreekt.
            if not proposed_country:
                continue

        if not _customs_changed(mirror_hs, mirror_country, proposed_hs, proposed_country):
            continue

        source_parts: list[str] = []
        if proposed_hs:
            source_parts.append(f"type_map:{mapped_source or 'customs_type_hs_mapping'}")
        if proposed_country:
            source_parts.append("coo_default_missing")
        source = "|".join(source_parts) or "customs_missing_fill"

        notes = (
            f"customs missing-fill hs/country {(mirror_hs or 'NULL')}/{(mirror_country or 'NULL')} -> "
            f"{(proposed_hs or mirror_hs or 'NULL')}/{(proposed_country or mirror_country or 'NULL')}; "
            f"source={source}"
        )

        rows_out.append(
            {
                "batch_id": str(batch_id),
                "sku": sku or None,
                "shopify_variant_id": vid,
                "shopify_product_id": pid,
                "mirror_inventory_item_id": inv_item,
                "mirror_hs_code": mirror_hs,
                "mirror_country_of_origin": mirror_country,
                "proposed_hs_code": proposed_hs,
                "proposed_country_of_origin": proposed_country,
                "customs_source": source,
                "customs_confidence": "type_table_missing_fill",
                "price_changed": False,
                "eta_changed": False,
                "status_changed": False,
                "inventory_policy_changed": False,
                "customs_changed": True,
                "notes": notes,
            }
        )

    review_path = Path(args.review_csv)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with open(review_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["shopify_variant_id", "sku", "product_type", "reason"],
            delimiter=";",
        )
        writer.writeheader()
        for rr in review_rows:
            writer.writerow(rr)

    print(
        "Customs missing-fill: "
        f"matching_missing={seen_missing}, staged={len(rows_out)}, review_rows={len(review_rows)}, "
        f"batch_id={batch_id}",
        flush=True,
    )
    print(f"Review CSV: {review_path}", flush=True)

    if args.dry_run:
        for r in rows_out[:25]:
            print(json.dumps(r, default=str), flush=True)
        if len(rows_out) > 25:
            print(f"... en {len(rows_out) - 25} rows meer", flush=True)
        return 0

    if not rows_out:
        print("Geen rows om te inserten in pricelist_sync_staging.", flush=True)
        return 0

    chunk_size = 300
    url = f"{base}/pricelist_sync_staging"
    for i in range(0, len(rows_out), chunk_size):
        chunk = rows_out[i : i + chunk_size]
        r = sess.post(
            url,
            headers=headers_wr,
            data=json.dumps(chunk),
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        print(f"Insert {min(i + chunk_size, len(rows_out))}/{len(rows_out)}", flush=True)

    print(
        f"Klaar. Staging gevuld voor customs missing-fill, batch_id={batch_id}.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
