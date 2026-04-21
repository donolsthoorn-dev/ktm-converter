#!/usr/bin/env python3
"""
Vergelijk KTM prijs-CSV('s) in input/ met de Supabase-spiegel (shopify_variants, shopify_eta,
shopify_products) en schrijf afwijkingen naar public.pricelist_sync_staging.

Geen Shopify API-calls. Bedoeld ter **handmatige review**; daarna pas (later/apply-workflow)
mutaties naar Shopify.

Vereist: Supabase-URL en service role (via ``load_project_env()``: zie
``modules/env_loader.py``).
Optioneel: dezelfde --csv / input/ defaults als shopify_sync_from_pricelist_csv.py

Migraties:
  - converter/supabase/migrations/002_pricelist_sync_staging.sql
  - converter/supabase/migrations/015_shopify_variants_customs_fields.sql
  - converter/supabase/migrations/016_pricelist_sync_staging_customs.sql

Tip: draai eerst de catalogus-mirror (job worker) zodat shopify_* actueel is.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
import uuid
from datetime import date
from decimal import Decimal
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

import config  # noqa: E402
from modules.customs_mapping import (  # noqa: E402
    load_external_customs_map,
    load_xml_customs_map,
    merge_customs_sources,
    normalize_country_code,
    normalize_hs_code,
    parse_allowed_hs_lengths,
)
from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

_REQUEST_TIMEOUT = (30, 120)
_PAGE = 1000


def _load_sync_module():
    path = ROOT / "scripts" / "shopify_sync_from_pricelist_csv.py"
    spec = importlib.util.spec_from_file_location("shopify_sync_from_pricelist_csv", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Kan module niet laden: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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
                f"Supabase GET {table} → HTTP {e.response.status_code}. "
                f"Controleer migraties (001 mirror, 002 pricelist_sync_staging) en RLS/service role. "
                f"Antwoord: {body}",
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


def _reset_staging_table(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    dry_run: bool,
) -> None:
    if dry_run:
        print("[dry-run] pricelist_sync_staging zou volledig geleegd worden.", flush=True)
        return
    # Volledige heropbouw per run: eerst alles weg uit staging.
    r = sess.delete(
        f"{base}/pricelist_sync_staging",
        headers=headers,
        params={"id": "not.is.null"},
        timeout=_REQUEST_TIMEOUT,
    )
    if not r.ok:
        print(
            f"Supabase reset staging fout {r.status_code}: {r.text[:2000]}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
    print("pricelist_sync_staging geleegd (full rebuild).", flush=True)


def _to_decimal_price(raw: object | None) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw)).quantize(Decimal("0.0001"))
    except Exception:
        return None


def _price_changed(mirror: Decimal | None, proposed: Decimal | None) -> bool:
    if proposed is None:
        return False
    mp = mirror.quantize(Decimal("0.01")) if mirror is not None else None
    pp = proposed.quantize(Decimal("0.01"))
    if mp is None:
        return True
    return mp != pp


def _eta_key(raw: object | None) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.strip()
        return s[:10] if s else None
    if hasattr(raw, "isoformat"):
        return raw.isoformat()[:10]
    return str(raw)[:10]


def _eta_changed(mirror: object | None, proposed_iso: str | None) -> bool:
    return _eta_key(mirror) != _eta_key(proposed_iso)


def _canonical_shop_status(raw: str | None) -> str:
    """Vergelijkbaar met CSV (ACTIVE|DRAFT); overige Shopify-status blijft expliciet (o.a. ARCHIVED)."""
    s = (raw or "").strip().upper()
    if s == "DRAFT":
        return "DRAFT"
    if s in ("ACTIVE", "PUBLISHED"):
        return "ACTIVE"
    if s == "ARCHIVED":
        return "ARCHIVED"
    return s or "ACTIVE"


def _status_changed(mirror: str | None, proposed: str) -> bool:
    return _canonical_shop_status(mirror) != proposed


def _canonical_inventory_policy(raw: str | None) -> str:
    s = (raw or "").strip().upper()
    if s == "DENY":
        return "DENY"
    if s == "CONTINUE":
        return "CONTINUE"
    return ""


def _inventory_policy_changed(mirror: str | None, proposed: str | None) -> bool:
    if not proposed:
        return False
    return _canonical_inventory_policy(mirror) != _canonical_inventory_policy(proposed)


def _customs_changed(
    mirror_hs: str | None,
    mirror_country: str | None,
    proposed_hs: str | None,
    proposed_country: str | None,
) -> bool:
    # Geen bronwaarde = geen mutatie forceren.
    if not proposed_hs and not proposed_country:
        return False
    if proposed_hs and (mirror_hs or "") != proposed_hs:
        return True
    if proposed_country and (mirror_country or "") != proposed_country:
        return True
    return False


def _fmt_decimal(v: Decimal | None) -> str:
    if v is None:
        return "NULL"
    return f"{v.quantize(Decimal('0.01'))}"


def _build_notes(
    mirror_price: Decimal | None,
    proposed_price: Decimal | None,
    mirror_eta: object | None,
    proposed_eta: str | None,
    mirror_status: str | None,
    proposed_status: str,
    article_status_code: str,
    stock_available_code: int | None,
    mirror_inventory_policy: str | None,
    proposed_inventory_policy: str | None,
    mirror_hs: str | None,
    proposed_hs: str | None,
    mirror_country: str | None,
    proposed_country: str | None,
    customs_source: str | None,
    price_changed: bool,
    eta_changed: bool,
    status_changed: bool,
    inventory_policy_changed: bool,
    customs_changed: bool,
) -> str:
    reasons: list[str] = []
    if price_changed:
        reasons.append(f"price {_fmt_decimal(mirror_price)} -> {_fmt_decimal(proposed_price)}")
    if eta_changed:
        reasons.append(f"eta {_eta_key(mirror_eta) or 'NULL'} -> {_eta_key(proposed_eta) or 'NULL'}")
    if status_changed:
        reasons.append(
            f"product_status {_canonical_shop_status(mirror_status)} -> {proposed_status}"
        )
    if inventory_policy_changed:
        reasons.append(
            f"inventory_policy {_canonical_inventory_policy(mirror_inventory_policy) or 'UNKNOWN'} -> "
            f"{_canonical_inventory_policy(proposed_inventory_policy) or 'UNKNOWN'}"
        )
    if customs_changed:
        if proposed_hs and proposed_country:
            reasons.append(
                f"customs hs/country {(mirror_hs or 'NULL')}/{(mirror_country or 'NULL')} -> "
                f"{proposed_hs}/{proposed_country}"
            )
        elif proposed_hs:
            reasons.append(f"customs hs {(mirror_hs or 'NULL')} -> {proposed_hs}")
        elif proposed_country:
            reasons.append(f"customs country {(mirror_country or 'NULL')} -> {proposed_country}")
        if customs_source:
            reasons.append(f"customs_source={customs_source}")
    code = article_status_code or "UNKNOWN"
    stock_txt = str(stock_available_code) if stock_available_code is not None else "UNKNOWN"
    if proposed_inventory_policy:
        reasons.append(
            f"Policy rule (hybrid): ArticleStatus={code}, StockAvailable={stock_txt} => "
            f"inventory_policy={_canonical_inventory_policy(proposed_inventory_policy)}"
        )
    else:
        reasons.append(
            f"Policy rule: geen afleiding (ArticleStatus={code}, StockAvailable={stock_txt})"
        )
    return "; ".join(reasons)


def main() -> int:
    sync = _load_sync_module()
    sync.load_dotenv()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--csv",
        action="append",
        dest="csv_paths",
        metavar="PAD",
        help="Zelfde als shopify_sync_from_pricelist_csv (herhaalbaar)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Geen Supabase-insert; wel tellingen en voorbeeldregels",
    )
    p.add_argument(
        "--batch-id",
        type=uuid.UUID,
        default=None,
        help="Vaste batch UUID (default: nieuwe uuid4)",
    )
    p.add_argument(
        "--missing-skus-csv",
        default=str(ROOT / "output" / "pricelist_missing_in_shopify.csv"),
        help="Pad voor rapport met CSV-SKU's zonder match in shopify_variants (leeg = niet schrijven)",
    )
    p.add_argument(
        "--customs-map-csv",
        default="",
        help=(
            "Optionele externe mapping CSV (SKU -> HS/country). "
            "Kolommen: sku + hs_code/customs_no + country_of_origin/origin."
        ),
    )
    p.add_argument(
        "--customs-report-csv",
        default=str(ROOT / "output" / "pricelist_customs_mapping_report.csv"),
        help="Pad voor customs mapping-rapport (leeg = niet schrijven)",
    )
    p.add_argument(
        "--allowed-hs-lengths",
        default=os.environ.get("SHOPIFY_CUSTOMS_ALLOWED_HS_LENGTHS", "6,8,10"),
        help="Toegestane HS-code lengtes na normalisatie (comma-separated, default: 6,8,10)",
    )
    p.add_argument(
        "--xml-path",
        default="",
        help="Optioneel XML-pad voor customs-attributen (default: config.XML_FILE)",
    )
    args = p.parse_args()

    batch_id = args.batch_id or uuid.uuid4()
    today = date.today()
    try:
        csv_paths = sync.resolve_csv_paths(args.csv_paths)
    except FileNotFoundError as e:
        print(
            f"Geen prijs-CSV in input/: {e}\n"
            "Controleer FTP (KTM_SFTP_*), of prepare_input_from_ftp bestanden kopieert "
            "(KTM_SFTP_FILES / KTM_PREPARE_FILES) en of downloads/ftp de CSV’s bevat.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    desired_by_sku = sync.read_pricelist_csv_desired_many(csv_paths, today)
    desired_product_status_by_pid: dict[str, str] = {}
    allowed_hs_lengths = parse_allowed_hs_lengths(args.allowed_hs_lengths)

    print(f"Batch: {batch_id}", flush=True)
    print(f"CSV-bronnen: {len(csv_paths)} bestand(en), {len(desired_by_sku)} unieke SKU's na merge", flush=True)

    xml_path_raw = (args.xml_path or "").strip()
    xml_path = Path(xml_path_raw) if xml_path_raw else Path(config.XML_FILE)
    xml_map: dict[str, dict[str, str]] = {}
    xml_rejected: list[dict[str, str]] = []
    xml_rows = 0
    if xml_path.is_file():
        xml_map, xml_rejected, xml_rows = load_xml_customs_map(xml_path, allowed_hs_lengths)
    else:
        print(f"Waarschuwing: XML bestand niet gevonden voor customs: {xml_path}", flush=True)

    external_map: dict[str, dict[str, str]] = {}
    external_rejected: list[dict[str, str]] = []
    external_rows = 0
    customs_map_csv = (args.customs_map_csv or "").strip()
    if customs_map_csv:
        ext_path = Path(customs_map_csv)
        if not ext_path.is_file():
            print(f"Waarschuwing: customs-map CSV ontbreekt: {ext_path}", flush=True)
        else:
            external_map, external_rejected, external_rows = load_external_customs_map(
                ext_path, allowed_hs_lengths
            )

    merged_customs, customs_report_rows = merge_customs_sources(
        set(desired_by_sku.keys()), xml_map, external_map
    )
    for sku, custom in merged_customs.items():
        desired_by_sku[sku]["hs_code"] = custom.get("hs_code") or None
        desired_by_sku[sku]["country_of_origin"] = custom.get("country_of_origin") or None
        desired_by_sku[sku]["customs_source"] = custom.get("source") or None
        desired_by_sku[sku]["customs_confidence"] = custom.get("tier") or None

    print(
        "Customs brondekking: "
        f"XML sku_rows={xml_rows}, xml_valid={len(xml_map)}, xml_invalid={len(xml_rejected)}; "
        f"external_rows={external_rows}, external_valid={len(external_map)}, external_invalid={len(external_rejected)}; "
        f"resolved_for_desired={len(merged_customs)}/{len(desired_by_sku)}",
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
            "shopify_variant_id,shopify_product_id,sku,price,inventory_policy,"
            "inventory_item_id,harmonized_system_code,country_code_of_origin"
        ),
        order="shopify_variant_id.asc",
    )
    print(f"  → {len(variants)} variant-rijen", flush=True)

    print("Supabase: shopify_products ophalen…", flush=True)
    products = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_products",
        "shopify_product_id,status",
        order="shopify_product_id.asc",
    )
    status_by_pid: dict[int, str] = {}
    for row in products:
        pid = row.get("shopify_product_id")
        if pid is not None:
            status_by_pid[int(pid)] = str(row.get("status") or "")

    print("Supabase: shopify_eta ophalen…", flush=True)
    etas = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_eta",
        "shopify_variant_id,eta_date",
        order="shopify_variant_id.asc",
    )
    eta_by_vid: dict[int, Any] = {}
    for row in etas:
        vid = row.get("shopify_variant_id")
        if vid is not None:
            eta_by_vid[int(vid)] = row.get("eta_date")

    by_sku: dict[str, list[dict[str, Any]]] = {}
    for v in variants:
        sku = (v.get("sku") or "").strip().upper()
        if not sku:
            continue
        by_sku.setdefault(sku, []).append(v)

    if hasattr(sync, "resolve_desired_product_status_by_product_id"):
        cache_for_status: dict[str, list[tuple[str, str | None]]] = {}
        for sku, vrows in by_sku.items():
            cache_for_status[sku] = [
                (str(v["shopify_variant_id"]), str(v["shopify_product_id"]) if v.get("shopify_product_id") is not None else None)
                for v in vrows
            ]
        desired_product_status_by_pid = sync.resolve_desired_product_status_by_product_id(
            desired_by_sku, cache_for_status
        )

    rows_out: list[dict[str, Any]] = []
    missing_mirror = 0
    missing_rows: list[dict[str, Any]] = []

    for sku, d in sorted(desired_by_sku.items()):
        vrows = by_sku.get(sku)
        if not vrows:
            missing_mirror += 1
            missing_rows.append(
                {
                    "sku": sku,
                    "proposed_eta_date": d.get("eta_iso"),
                    "proposed_price": d.get("price_incl"),
                    "proposed_product_status": d.get("product_status"),
                    "proposed_hs_code": d.get("hs_code"),
                    "proposed_country_of_origin": d.get("country_of_origin"),
                    "customs_source": d.get("customs_source"),
                    "proposed_article_status_code": d.get("article_status_code"),
                    "proposed_stock_available_code": d.get("stock_available_code"),
                    "reason": "sku_not_found_in_shopify_variants_mirror",
                }
            )
            continue

        prop_price = _to_decimal_price(d.get("price_incl"))
        prop_eta = d.get("eta_iso")
        prop_article_status = str(d.get("article_status_code") or "").strip()
        prop_stock_available_code_raw = d.get("stock_available_code")
        prop_stock_available_code: int | None = None
        if isinstance(prop_stock_available_code_raw, int):
            prop_stock_available_code = prop_stock_available_code_raw
        prop_inventory_policy = str(d.get("inventory_policy") or "").strip().upper() or None
        prop_sell_when_out_of_stock = None
        if prop_inventory_policy:
            prop_sell_when_out_of_stock = prop_inventory_policy == "CONTINUE"
        prop_hs = normalize_hs_code(d.get("hs_code"), allowed_hs_lengths)
        prop_country = normalize_country_code(d.get("country_of_origin"))
        customs_source = str(d.get("customs_source") or "").strip() or None
        customs_confidence = str(d.get("customs_confidence") or "").strip() or None

        for v in vrows:
            vid = int(v["shopify_variant_id"])
            pid = int(v["shopify_product_id"]) if v.get("shopify_product_id") is not None else None
            mirror_p = _to_decimal_price(v.get("price"))
            mirror_eta = eta_by_vid.get(vid)
            mirror_stat = status_by_pid.get(pid) if pid is not None else None
            mirror_policy = v.get("inventory_policy")
            mirror_item_id = (
                int(v["inventory_item_id"]) if v.get("inventory_item_id") is not None else None
            )
            mirror_hs = normalize_hs_code(v.get("harmonized_system_code"), allowed_hs_lengths)
            mirror_country = normalize_country_code(v.get("country_code_of_origin"))
            if pid is not None and str(pid) in desired_product_status_by_pid:
                prop_stat = desired_product_status_by_pid[str(pid)]
            else:
                # Deze flow zet product niet meer naar DRAFT; bij all-80 blijft productstatus ongewijzigd.
                prop_stat = _canonical_shop_status(mirror_stat)

            pc = _price_changed(mirror_p, prop_price)
            ec = _eta_changed(mirror_eta, prop_eta)
            sc = _status_changed(mirror_stat, prop_stat)
            ic = _inventory_policy_changed(mirror_policy, prop_inventory_policy)
            cc = _customs_changed(
                mirror_hs,
                mirror_country,
                prop_hs,
                prop_country,
            )

            if not (pc or ec or sc or ic or cc):
                continue

            row: dict[str, Any] = {
                "batch_id": str(batch_id),
                "sku": sku,
                "shopify_variant_id": vid,
                "shopify_product_id": pid,
                "mirror_inventory_item_id": mirror_item_id,
                "mirror_price": float(mirror_p) if mirror_p is not None else None,
                "mirror_eta_date": _eta_key(mirror_eta),
                "mirror_product_status": _canonical_shop_status(mirror_stat),
                "mirror_hs_code": mirror_hs,
                "mirror_country_of_origin": mirror_country,
                "proposed_price": float(prop_price) if prop_price is not None else None,
                "proposed_eta_date": _eta_key(prop_eta),
                "proposed_product_status": prop_stat,
                "proposed_hs_code": prop_hs,
                "proposed_country_of_origin": prop_country,
                "customs_source": customs_source,
                "customs_confidence": customs_confidence,
                "mirror_inventory_policy": _canonical_inventory_policy(str(mirror_policy) if mirror_policy is not None else None) or None,
                "proposed_inventory_policy": prop_inventory_policy,
                "proposed_sell_when_out_of_stock": prop_sell_when_out_of_stock,
                "proposed_article_status_code": prop_article_status,
                "price_changed": pc,
                "eta_changed": ec,
                "status_changed": sc,
                "inventory_policy_changed": ic,
                "customs_changed": cc,
                "notes": _build_notes(
                    mirror_p,
                    prop_price,
                    mirror_eta,
                    prop_eta,
                    mirror_stat,
                    prop_stat,
                    prop_article_status,
                    prop_stock_available_code,
                    mirror_policy,
                    prop_inventory_policy,
                    mirror_hs,
                    prop_hs,
                    mirror_country,
                    prop_country,
                    customs_source,
                    pc,
                    ec,
                    sc,
                    ic,
                    cc,
                ),
            }
            rows_out.append(row)

    print(
        f"Te schrijven staging-rijen (minstens één verschil): {len(rows_out)}",
        flush=True,
    )
    print(f"SKU's in CSV zonder match in shopify_variants: {missing_mirror}", flush=True)
    out_missing_csv = (args.missing_skus_csv or "").strip()
    if out_missing_csv:
        out_path = Path(out_missing_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fields = [
            "sku",
            "proposed_eta_date",
            "proposed_price",
            "proposed_product_status",
            "proposed_hs_code",
            "proposed_country_of_origin",
            "customs_source",
            "proposed_article_status_code",
            "proposed_stock_available_code",
            "reason",
        ]
        with open(out_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            for row in missing_rows:
                writer.writerow(row)
        print(f"Rapport missing SKUs geschreven: {out_path} ({len(missing_rows)} rijen)", flush=True)

    out_customs_csv = (args.customs_report_csv or "").strip()
    if out_customs_csv:
        customs_path = Path(out_customs_csv)
        customs_path.parent.mkdir(parents=True, exist_ok=True)
        report_fields = [
            "sku",
            "resolved",
            "hs_code",
            "country_of_origin",
            "tier",
            "source",
            "reason",
        ]
        with open(customs_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=report_fields)
            writer.writeheader()
            for row in customs_report_rows:
                writer.writerow(row)
            for row in xml_rejected:
                writer.writerow(
                    {
                        "sku": row.get("sku", ""),
                        "resolved": "0",
                        "hs_code": "",
                        "country_of_origin": "",
                        "tier": "xml_invalid",
                        "source": "xml",
                        "reason": row.get("reason", "xml_invalid"),
                    }
                )
            for row in external_rejected:
                writer.writerow(
                    {
                        "sku": row.get("sku", ""),
                        "resolved": "0",
                        "hs_code": "",
                        "country_of_origin": "",
                        "tier": "external_invalid",
                        "source": "external",
                        "reason": row.get("reason", "external_invalid"),
                    }
                )
        print(
            f"Customs mapping-rapport geschreven: {customs_path} ({len(customs_report_rows)} basisrijen)",
            flush=True,
        )

    if args.dry_run:
        for i, row in enumerate(rows_out[:25]):
            print(f"  [dry-run] {json.dumps(row, default=str)}", flush=True)
        if len(rows_out) > 25:
            print(f"  … en {len(rows_out) - 25} meer", flush=True)
        return 0

    if not rows_out:
        print("Niets te inserten (staging is wel leeg gemaakt voor deze run).", flush=True)
        return 0

    # Bulk insert in chunks (PostgREST)
    url = f"{base}/pricelist_sync_staging"
    chunk_size = 300
    for i in range(0, len(rows_out), chunk_size):
        chunk = rows_out[i : i + chunk_size]
        r = sess.post(
            url,
            headers=headers,
            data=json.dumps(chunk),
            timeout=_REQUEST_TIMEOUT,
        )
        if not r.ok:
            print(f"Supabase insert fout {r.status_code}: {r.text[:2000]}", file=sys.stderr, flush=True)
            return 1
        print(f"  Insert {min(i + chunk_size, len(rows_out))}/{len(rows_out)}", flush=True)

    print(
        f"Klaar. Review in Supabase: table pricelist_sync_staging, batch_id = {batch_id}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
