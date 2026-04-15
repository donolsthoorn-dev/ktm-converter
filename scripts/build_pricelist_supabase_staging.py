#!/usr/bin/env python3
"""
Vergelijk KTM prijs-CSV('s) in input/ met de Supabase-spiegel (shopify_variants, shopify_eta,
shopify_products) en schrijf afwijkingen naar public.pricelist_sync_staging.

Geen Shopify API-calls. Bedoeld ter **handmatige review**; daarna pas (later/apply-workflow)
mutaties naar Shopify.

Vereist: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
Optioneel: dezelfde --csv / input/ defaults als shopify_sync_from_pricelist_csv.py

Migratie: converter/supabase/migrations/002_pricelist_sync_staging.sql

Tip: draai eerst de catalogus-mirror (job worker) zodat shopify_* actueel is.
"""

from __future__ import annotations

import argparse
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

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

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
    proposed_published: bool,
    price_changed: bool,
    eta_changed: bool,
    status_changed: bool,
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
    code = article_status_code or "UNKNOWN"
    if code == "80":
        reasons.append("ArticleStatus=80 => published=false (draft)")
    else:
        reasons.append(f"ArticleStatus={code} => published={'true' if proposed_published else 'false'}")
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

    print(f"Batch: {batch_id}", flush=True)
    print(f"CSV-bronnen: {len(csv_paths)} bestand(en), {len(desired_by_sku)} unieke SKU's na merge", flush=True)

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
        "shopify_variant_id,shopify_product_id,sku,price",
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

    for sku, d in sorted(desired_by_sku.items()):
        vrows = by_sku.get(sku)
        if not vrows:
            missing_mirror += 1
            continue

        prop_price = _to_decimal_price(d.get("price_incl"))
        prop_eta = d.get("eta_iso")
        prop_article_status = str(d.get("article_status_code") or "").strip()
        prop_published = bool(d.get("published", prop_article_status != "80"))

        for v in vrows:
            vid = int(v["shopify_variant_id"])
            pid = int(v["shopify_product_id"]) if v.get("shopify_product_id") is not None else None
            prop_stat = (
                desired_product_status_by_pid.get(str(pid), str(d.get("product_status") or "ACTIVE"))
                if pid is not None
                else str(d.get("product_status") or "ACTIVE")
            )
            mirror_p = _to_decimal_price(v.get("price"))
            mirror_eta = eta_by_vid.get(vid)
            mirror_stat = status_by_pid.get(pid) if pid is not None else None

            pc = _price_changed(mirror_p, prop_price)
            ec = _eta_changed(mirror_eta, prop_eta)
            sc = _status_changed(mirror_stat, prop_stat)

            if not (pc or ec or sc):
                continue

            row: dict[str, Any] = {
                "batch_id": str(batch_id),
                "sku": sku,
                "shopify_variant_id": vid,
                "shopify_product_id": pid,
                "mirror_price": float(mirror_p) if mirror_p is not None else None,
                "mirror_eta_date": _eta_key(mirror_eta),
                "mirror_product_status": _canonical_shop_status(mirror_stat),
                "proposed_price": float(prop_price) if prop_price is not None else None,
                "proposed_eta_date": _eta_key(prop_eta),
                "proposed_product_status": prop_stat,
                "proposed_article_status_code": prop_article_status,
                "proposed_published": prop_published,
                "price_changed": pc,
                "eta_changed": ec,
                "status_changed": sc,
                "notes": _build_notes(
                    mirror_p,
                    prop_price,
                    mirror_eta,
                    prop_eta,
                    mirror_stat,
                    prop_stat,
                    prop_article_status,
                    prop_published,
                    pc,
                    ec,
                    sc,
                ),
            }
            rows_out.append(row)

    print(
        f"Te schrijven staging-rijen (minstens één verschil): {len(rows_out)}",
        flush=True,
    )
    print(f"SKU's in CSV zonder match in shopify_variants: {missing_mirror}", flush=True)

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
