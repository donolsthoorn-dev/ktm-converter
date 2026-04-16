#!/usr/bin/env python3
"""
Pas Shopify mutaties toe op basis van public.pricelist_sync_staging.

Deze flow leest de reeds berekende delta uit Supabase staging en voert alleen die mutaties uit:
  - variantprijs (price_changed)
  - ETA metafield set/clear (eta_changed)
  - variant inventory_policy (inventory_policy_changed)
  - productstatus (status_changed; in huidige flow vooral re-activatie naar ACTIVE)

Vereist:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
  SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN
Optioneel:
  SHOPIFY_ADMIN_API_VERSION (default 2024-10)
  SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE / SHOPIFY_VARIANT_ETA_METAFIELD_KEY
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

_REQUEST_TIMEOUT = (30, 120)
_PAGE = 1000
_STAMP_FLUSH_CHUNK = 250


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_benign_eta_clear_error(err: dict[str, Any]) -> bool:
    """
    Clearing a metafield is idempotent: if it is already absent, Shopify may return a userError.
    Treat those as non-fatal for sync purposes.
    """
    msg = str(err.get("message") or "").lower()
    return (
        "not found" in msg
        or "doesn't exist" in msg
        or "does not exist" in msg
        or "was not found" in msg
        or "could not be found" in msg
    )


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
    where: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        params: dict[str, str] = {
            "select": select,
            "limit": str(_PAGE),
            "offset": str(offset),
            "order": "shopify_variant_id.asc",
        }
        if where:
            params.update(where)
        r = sess.get(
            f"{base}/{table}",
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < _PAGE:
            break
        offset += _PAGE
    return out


def _supabase_upsert(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    table: str,
    rows: list[dict[str, Any]],
    on_conflict: str,
) -> None:
    if not rows:
        return
    h = {**headers, "Prefer": "resolution=merge-duplicates,return=minimal"}
    r = sess.post(
        f"{base}/{table}",
        params={"on_conflict": on_conflict},
        headers=h,
        json=rows,
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()


def _flush_staging_timestamps(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    column_name: str,
    stamp_by_row_id: dict[str, str],
) -> None:
    """Schrijf succesvolle Shopify-updates terug naar staging (per type timestamp)."""
    if not stamp_by_row_id:
        return
    items = list(stamp_by_row_id.items())
    for i in range(0, len(items), _STAMP_FLUSH_CHUNK):
        chunk = items[i : i + _STAMP_FLUSH_CHUNK]
        rows = [{"id": rid, column_name: ts} for rid, ts in chunk]
        _supabase_upsert(sess, base, headers, "pricelist_sync_staging", rows, "id")


def main() -> int:
    sync = _load_sync_module()
    sync.load_dotenv()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="Geen Shopify writes")
    p.add_argument("--batch-id", default="", help="Optioneel: alleen 1 batch_id toepassen")
    p.add_argument(
        "--scope",
        choices=("all", "price_eta", "policy"),
        default="all",
        help="Welke mutaties toepassen: all (default), price_eta, of policy",
    )
    args = p.parse_args()

    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
    api_ver = ((os.environ.get("SHOPIFY_ADMIN_API_VERSION") or "").strip() or "2024-10")
    ns = ((os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE") or "").strip() or "global")
    key_mf = (
        (os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_KEY") or "").strip()
        or "inventory_policy_eta_date"
    )
    print(f"ETA metafield: namespace={ns!r}, key={key_mf!r}", flush=True)
    if len(key_mf) < 2:
        print(
            "Ongeldige SHOPIFY_VARIANT_ETA_METAFIELD_KEY (te kort/leeg); stop om massale userErrors te voorkomen.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    if not args.dry_run and not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt", file=sys.stderr)
        return 1
    product_status_sleep_sec = float(
        (os.environ.get("SHOPIFY_PRODUCT_STATUS_SLEEP_SEC") or "").strip() or "0.25"
    )

    base = _rest_base()
    headers = _headers()
    sess = requests.Session()
    sess.trust_env = False

    where: dict[str, str] = {}
    if args.batch_id.strip():
        where["batch_id"] = f"eq.{args.batch_id.strip()}"
        print(f"Batch filter: {args.batch_id.strip()}", flush=True)

    rows = _fetch_paginated(
        sess,
        base,
        headers,
        "pricelist_sync_staging",
        (
            "id,sku,shopify_variant_id,shopify_product_id,"
            "proposed_price,proposed_eta_date,proposed_product_status,proposed_inventory_policy,"
            "price_changed,eta_changed,status_changed,inventory_policy_changed"
        ),
        where=where,
    )
    if not rows:
        print("Geen staging-rijen gevonden om toe te passen.", flush=True)
        return 0

    price_ops: list[tuple[str, str, str]] = []
    eta_set: list[tuple[str, str, str]] = []
    eta_clear: list[tuple[str, str]] = []
    policy_ops: list[tuple[str, str, str]] = []
    product_ops_by_pid: dict[str, tuple[str, str]] = {}
    variant_to_product_id: dict[str, str] = {}
    variant_to_staging_row_id: dict[str, str] = {}

    for r in rows:
        sku = str(r.get("sku") or "").strip().upper()
        sid = str(r.get("id") or "").strip()
        vid = str(r.get("shopify_variant_id") or "").strip()
        pid = str(r.get("shopify_product_id") or "").strip()
        if vid and pid:
            variant_to_product_id[vid] = pid
        if vid and sid:
            variant_to_staging_row_id[vid] = sid

        if r.get("price_changed") and vid:
            pp = r.get("proposed_price")
            if pp is not None:
                price_ops.append((sku, vid, f"{float(pp):.2f}"))

        if r.get("eta_changed") and vid:
            pe = r.get("proposed_eta_date")
            if pe:
                eta_set.append((sku, vid, str(pe)[:10]))
            else:
                eta_clear.append((sku, vid))

        if r.get("inventory_policy_changed") and vid:
            pol = (str(r.get("proposed_inventory_policy") or "").strip().upper() or "DENY")
            if pol in ("DENY", "CONTINUE"):
                policy_ops.append((sku, vid, pol))

        if r.get("status_changed") and pid:
            ps = str(r.get("proposed_product_status") or "").strip().upper() or "ACTIVE"
            product_ops_by_pid[pid] = (sku, ps)

    print(
        f"Staging delta: prijs {len(price_ops)}, eta_set {len(eta_set)}, eta_clear {len(eta_clear)}, "
        f"variant_policy {len(policy_ops)}, product_status {len(product_ops_by_pid)}",
        flush=True,
    )

    if args.dry_run:
        print("Dry-run: geen mutaties naar Shopify.", flush=True)
        return 0

    run_price_eta = args.scope in ("all", "price_eta")
    run_policy = args.scope in ("all", "policy")
    print(f"Apply scope: {args.scope}", flush=True)

    errors = 0
    benign = 0
    progress_every = 250
    eta_success: list[tuple[str, str | None]] = []
    price_success: list[tuple[str, str]] = []
    policy_success: list[tuple[str, str]] = []
    product_success: list[tuple[str, str]] = []
    price_stamp_by_row_id: dict[str, str] = {}
    eta_stamp_by_row_id: dict[str, str] = {}
    policy_stamp_by_row_id: dict[str, str] = {}

    # ETA mutaties in batches.
    batch_size = 25
    if run_price_eta:
        all_eta_ops = [("set", x) for x in eta_set] + [("clear", x) for x in eta_clear]
        i = 0
        b = 0
        while i < len(all_eta_ops):
            kind = all_eta_ops[i][0]
            batch: list[tuple[str, tuple[str, str] | tuple[str, str, str]]] = []
            while i < len(all_eta_ops) and len(batch) < batch_size and all_eta_ops[i][0] == kind:
                batch.append(all_eta_ops[i])
                i += 1
            b += 1
            if kind == "clear":
                identifiers = [
                    {
                        "ownerId": f"gid://shopify/ProductVariant/{op[1][1]}",
                        "namespace": ns,
                        "key": key_mf,
                    }
                    for op in batch
                ]
                data = sync.graphql_metafields_delete(shop, token, api_ver, identifiers)
                uerr = ((data or {}).get("metafieldsDelete") or {}).get("userErrors") or []
                for err in uerr:
                    if _is_benign_eta_clear_error(err):
                        benign += 1
                        continue
                    errors += 1
                    if errors <= 20:
                        print(f"ETA clear userError: {err}", flush=True)
                if not uerr:
                    for op in batch:
                        vid = str(op[1][1])
                        eta_success.append((vid, None))
                        sid = variant_to_staging_row_id.get(vid)
                        if sid:
                            eta_stamp_by_row_id[sid] = _iso_now()
                    _flush_staging_timestamps(
                        sess, base, headers, "eta_updated_at", eta_stamp_by_row_id
                    )
                    eta_stamp_by_row_id.clear()
            else:
                mfs = [
                    {
                        "ownerId": f"gid://shopify/ProductVariant/{op[1][1]}",
                        "namespace": ns,
                        "key": key_mf,
                        "type": "date",
                        "value": op[1][2],
                    }
                    for op in batch
                ]
                data = sync.graphql_metafields_set(shop, token, api_ver, mfs)
                uerr = ((data or {}).get("metafieldsSet") or {}).get("userErrors") or []
                for err in uerr:
                    errors += 1
                    if errors <= 20:
                        print(f"ETA set userError: {err}", flush=True)
                if not uerr:
                    for op in batch:
                        vid = str(op[1][1])
                        eta_success.append((vid, str(op[1][2])))
                        sid = variant_to_staging_row_id.get(vid)
                        if sid:
                            eta_stamp_by_row_id[sid] = _iso_now()
                    _flush_staging_timestamps(
                        sess, base, headers, "eta_updated_at", eta_stamp_by_row_id
                    )
                    eta_stamp_by_row_id.clear()
            if b == 1 or b % 25 == 0:
                print(f"ETA batch {b}: type={kind}, size={len(batch)}", flush=True)
    else:
        print("Skip ETA (scope zonder price_eta).", flush=True)

    # Prijs mutaties
    if run_price_eta:
        price_sess = sync._http_session()
        for idx, (sku, vid, price) in enumerate(price_ops, start=1):
            if not sync.rest_variant_price(shop, token, api_ver, vid, price, sess=price_sess):
                errors += 1
            else:
                price_success.append((vid, price))
                sid = variant_to_staging_row_id.get(vid)
                if sid:
                    price_stamp_by_row_id[sid] = _iso_now()
                if len(price_stamp_by_row_id) >= _STAMP_FLUSH_CHUNK:
                    _flush_staging_timestamps(
                        sess, base, headers, "price_updated_at", price_stamp_by_row_id
                    )
                    price_stamp_by_row_id.clear()
            if idx == 1 or idx % progress_every == 0 or idx == len(price_ops):
                print(f"Prijs {idx}/{len(price_ops)}", flush=True)
        _flush_staging_timestamps(sess, base, headers, "price_updated_at", price_stamp_by_row_id)
        price_stamp_by_row_id.clear()
    else:
        print("Skip prijs (scope zonder price_eta).", flush=True)

    # Variant inventory policy
    if run_policy:
        policy_sess = sync._http_session()
        for idx, (_sku, vid, pol) in enumerate(policy_ops, start=1):
            if not sync.rest_variant_inventory_policy(
                shop, token, api_ver, vid, pol, sess=policy_sess
            ):
                errors += 1
            else:
                policy_success.append((vid, pol))
                sid = variant_to_staging_row_id.get(vid)
                if sid:
                    policy_stamp_by_row_id[sid] = _iso_now()
                if len(policy_stamp_by_row_id) >= _STAMP_FLUSH_CHUNK:
                    _flush_staging_timestamps(
                        sess, base, headers, "policy_updated_at", policy_stamp_by_row_id
                    )
                    policy_stamp_by_row_id.clear()
            if idx == 1 or idx % progress_every == 0 or idx == len(policy_ops):
                print(f"Variant policy {idx}/{len(policy_ops)}", flush=True)
        _flush_staging_timestamps(
            sess, base, headers, "policy_updated_at", policy_stamp_by_row_id
        )
        policy_stamp_by_row_id.clear()
    else:
        print("Skip variant policy (scope zonder policy).", flush=True)

    # Product status (dedupe per product)
    if run_policy:
        product_sess = sync._http_session()
        deduped = [(pid, sku_ps[1]) for pid, sku_ps in product_ops_by_pid.items()]
        if deduped:
            print(
                f"Product status pacing: {product_status_sleep_sec:.2f}s tussen requests",
                flush=True,
            )
        for idx, (pid, ps) in enumerate(deduped, start=1):
            st_rest = "draft" if ps == "DRAFT" else "active"
            if not sync.rest_product_status(shop, token, api_ver, pid, st_rest, sess=product_sess):
                errors += 1
            else:
                product_success.append((pid, st_rest.upper()))
            if idx == 1 or idx % progress_every == 0 or idx == len(deduped):
                print(f"Product status {idx}/{len(deduped)}", flush=True)
            if product_status_sleep_sec > 0:
                time.sleep(product_status_sleep_sec)
    else:
        print("Skip product status (scope zonder policy).", flush=True)

    # Richt de Supabase mirror direct bij op basis van succesvolle mutaties.
    ts = _iso_now()
    try:
        if eta_success:
            eta_rows = [
                {
                    "shopify_variant_id": int(vid),
                    "eta_date": eta,
                    "eta_raw": eta,
                    "synced_at": ts,
                }
                for vid, eta in eta_success
            ]
            _supabase_upsert(sess, base, headers, "shopify_eta", eta_rows, "shopify_variant_id")

        if price_success or policy_success:
            by_vid: dict[str, dict[str, Any]] = {}
            for vid, price in price_success:
                row = by_vid.setdefault(vid, {"shopify_variant_id": int(vid), "synced_at": ts})
                row["price"] = price
            for vid, pol in policy_success:
                row = by_vid.setdefault(vid, {"shopify_variant_id": int(vid), "synced_at": ts})
                row["inventory_policy"] = pol
            variant_rows: list[dict[str, Any]] = []
            for vid, row in by_vid.items():
                pid = variant_to_product_id.get(vid)
                if pid:
                    row["shopify_product_id"] = int(pid)
                variant_rows.append(row)
            _supabase_upsert(
                sess, base, headers, "shopify_variants", variant_rows, "shopify_variant_id"
            )

        if product_success:
            product_rows = [
                {"shopify_product_id": int(pid), "status": st, "synced_at": ts}
                for pid, st in product_success
            ]
            _supabase_upsert(
                sess, base, headers, "shopify_products", product_rows, "shopify_product_id"
            )
    except requests.RequestException as e:
        errors += 1
        print(f"Mirror partial update fout: {e}", file=sys.stderr, flush=True)
        if e.response is not None:
            print((e.response.text or "")[:1500], file=sys.stderr, flush=True)

    if benign:
        print(f"Opmerking: {benign} idempotente ETA-clear meldingen genegeerd.", flush=True)
    print(f"Klaar. fouten={errors}", flush=True)
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
