#!/usr/bin/env python3
"""
Volledige catalogus customs staging (alle Shopify varianten) -> public.pricelist_sync_staging.

Doel:
- Niet alleen prijs-CSV delta, maar alle varianten in mirror beoordelen op customs velden.
- Country of origin geforceerd zetten naar een vaste waarde (default: AT).
- HS-code invullen vanuit externe mapping CSV op SKU (indien beschikbaar).
- Alleen echte verschillen schrijven met customs_changed=true.

Gebruik:
  python3 scripts/build_customs_supabase_staging_full_catalog.py --dry-run
  python3 scripts/build_customs_supabase_staging_full_catalog.py
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
    load_external_customs_map,
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


def _headers() -> dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        raise SystemExit("SUPABASE_SERVICE_ROLE_KEY ontbreekt")
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
        r.raise_for_status()
        rows = r.json() or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < _PAGE:
            break
        offset += _PAGE
    return out


def _reset_staging_table(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    dry_run: bool,
) -> None:
    if dry_run:
        print("[dry-run] pricelist_sync_staging zou volledig geleegd worden.", flush=True)
        return
    r = sess.delete(
        f"{base}/pricelist_sync_staging",
        headers=headers,
        params={"id": "not.is.null"},
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    print("pricelist_sync_staging geleegd (full rebuild).", flush=True)


def _customs_changed(
    mirror_hs: str | None,
    mirror_country: str | None,
    proposed_hs: str | None,
    proposed_country: str | None,
) -> bool:
    if not proposed_hs and not proposed_country:
        return False
    if proposed_hs and (mirror_hs or "") != proposed_hs:
        return True
    if proposed_country and (mirror_country or "") != proposed_country:
        return True
    return False


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--customs-map-csv",
        default=str(ROOT / "input" / "customs_mapping_types_batch1.csv"),
        help="Externe mapping CSV (sku;hs_code;country_of_origin;source)",
    )
    p.add_argument(
        "--country",
        default="AT",
        help="Geforceerde country_of_origin voor alle varianten (default: AT)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Geen staging insert; wel rapportage en voorbeeldrijen",
    )
    p.add_argument(
        "--batch-id",
        type=uuid.UUID,
        default=None,
        help="Vaste batch UUID (default: nieuwe uuid4)",
    )
    p.add_argument(
        "--allowed-hs-lengths",
        default=os.environ.get("SHOPIFY_CUSTOMS_ALLOWED_HS_LENGTHS", "6,8,10"),
        help="Toegestane HS-code lengtes na normalisatie (default: 6,8,10)",
    )
    p.add_argument(
        "--review-csv",
        default=str(ROOT / "output" / "customs_full_catalog_review.csv"),
        help="Rapport met overgeslagen varianten (geen sku/inventory item etc.)",
    )
    args = p.parse_args()

    forced_country = normalize_country_code(args.country)
    if not forced_country:
        raise SystemExit(f"Ongeldige country code: {args.country!r}")
    allowed_hs_lengths = parse_allowed_hs_lengths(args.allowed_hs_lengths)
    batch_id = args.batch_id or uuid.uuid4()

    map_path = Path(args.customs_map_csv)
    if not map_path.is_file():
        raise SystemExit(f"customs-map CSV niet gevonden: {map_path}")
    external_map, ext_rejected, ext_rows = load_external_customs_map(map_path, allowed_hs_lengths)
    print(
        f"Customs map: rows={ext_rows}, valid={len(external_map)}, invalid={len(ext_rejected)}",
        flush=True,
    )

    base = _rest_base()
    headers = _headers()
    sess = requests.Session()
    sess.trust_env = False

    _reset_staging_table(sess, base, headers, args.dry_run)

    print("Supabase: shopify_variants ophalen…", flush=True)
    variants = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_variants",
        (
            "shopify_variant_id,shopify_product_id,sku,inventory_item_id,"
            "harmonized_system_code,country_code_of_origin"
        ),
        order="shopify_variant_id.asc",
    )
    print(f"  -> {len(variants)} variant-rijen", flush=True)

    out_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, str]] = []
    changed = 0
    missing_sku = 0
    missing_inv_item = 0

    for row in variants:
        vid_raw = row.get("shopify_variant_id")
        pid_raw = row.get("shopify_product_id")
        sku = str(row.get("sku") or "").strip().upper()
        inv_item = row.get("inventory_item_id")
        if vid_raw is None or pid_raw is None:
            continue
        vid = int(vid_raw)
        pid = int(pid_raw)

        if not sku:
            missing_sku += 1
            review_rows.append(
                {
                    "shopify_variant_id": str(vid),
                    "sku": "",
                    "reason": "missing_sku",
                }
            )
            continue
        if inv_item is None:
            missing_inv_item += 1
            review_rows.append(
                {
                    "shopify_variant_id": str(vid),
                    "sku": sku,
                    "reason": "missing_inventory_item_id",
                }
            )
            continue

        mirror_hs = normalize_hs_code(row.get("harmonized_system_code"), allowed_hs_lengths)
        mirror_country = normalize_country_code(row.get("country_code_of_origin"))

        mapped = external_map.get(sku) or {}
        proposed_hs = normalize_hs_code(mapped.get("hs_code"), allowed_hs_lengths)
        proposed_country = forced_country  # expliciete gebruikerskeuze: altijd AT.
        source = (mapped.get("source") or "").strip() or "forced_country_full_catalog"
        confidence = str(mapped.get("tier") or "").strip() or ("external_exact" if proposed_hs else "forced_country_only")

        cc = _customs_changed(mirror_hs, mirror_country, proposed_hs, proposed_country)
        if not cc:
            continue
        changed += 1

        notes = (
            f"customs hs/country {(mirror_hs or 'NULL')}/{(mirror_country or 'NULL')} -> "
            f"{(proposed_hs or 'NULL')}/{proposed_country}; customs_source={source}"
        )
        out_rows.append(
            {
                "batch_id": str(batch_id),
                "sku": sku,
                "shopify_variant_id": vid,
                "shopify_product_id": pid,
                "mirror_inventory_item_id": int(inv_item),
                "mirror_hs_code": mirror_hs,
                "mirror_country_of_origin": mirror_country,
                "proposed_hs_code": proposed_hs,
                "proposed_country_of_origin": proposed_country,
                "customs_source": source,
                "customs_confidence": confidence,
                "price_changed": False,
                "eta_changed": False,
                "status_changed": False,
                "inventory_policy_changed": False,
                "customs_changed": True,
                "notes": notes,
            }
        )

    print(
        f"Customs full-catalog: changed_rows={changed}, missing_sku={missing_sku}, missing_inventory_item_id={missing_inv_item}",
        flush=True,
    )

    review_path = Path(args.review_csv)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with open(review_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["shopify_variant_id", "sku", "reason"],
            delimiter=";",
        )
        writer.writeheader()
        for rr in review_rows:
            writer.writerow(rr)
    print(f"Review-rapport geschreven: {review_path} ({len(review_rows)} rijen)", flush=True)

    if args.dry_run:
        for i, r in enumerate(out_rows[:25], start=1):
            print(f"  [dry-run] {json.dumps(r, default=str)}", flush=True)
        if len(out_rows) > 25:
            print(f"  ... en {len(out_rows) - 25} meer", flush=True)
        return 0

    if not out_rows:
        print("Niets te inserten in staging.", flush=True)
        return 0

    url = f"{base}/pricelist_sync_staging"
    chunk_size = 300
    for i in range(0, len(out_rows), chunk_size):
        chunk = out_rows[i : i + chunk_size]
        r = sess.post(
            url,
            headers=headers,
            data=json.dumps(chunk),
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        print(f"  Insert {min(i + chunk_size, len(out_rows))}/{len(out_rows)}", flush=True)

    print(
        f"Klaar. Review in Supabase: table pricelist_sync_staging, batch_id = {batch_id}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
