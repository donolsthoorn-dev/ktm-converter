#!/usr/bin/env python3
"""
Verwijder Motox-duplicaten met numerieke handle-suffix -1.

Alleen als het basisproduct (handle zonder -1) bestaat en minstens één SKU deelt.
Het basisproduct blijft staan.

Standaard dry-run, en standaard maar 5 producten. Eerste proef:

  python3 scripts/motox_delete_handle_dupes.py
  python3 scripts/motox_delete_handle_dupes.py --apply --limit 200

Een volgende run hervat automatisch na de laatst verwijderde handle, zodat
al weggegooide duplicaten niet opnieuw bij Shopify worden opgevraagd.
Er is geen nachtelijke job meer; de POS-kopie van 2024-09-13 is opgeruimd.

  python3 scripts/motox_delete_handle_dupes.py --apply --limit 200
  python3 scripts/motox_delete_handle_dupes.py --apply --limit 0
  python3 scripts/motox_delete_handle_dupes.py --refresh-cache --from-start --apply --limit 100
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.constants import OUTPUT_ROOT  # noqa: E402
from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402
from modules.motox_shopify_cache import (  # noqa: E402
    BULK_JSONL_FILE,
    CACHE_DIR,
    load_motox_indexes,
    refresh_motox_cache,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("motox_delete_handle_dupes")

# POS-kloon van 2024-09-13: Shopify zette -1 achter een handle die al bestond.
_HANDLE_DUPE = re.compile(r"^(\d{6,})-1$")
_SHOP = "ktm-shop-nederland"
_CURSOR_FILE = CACHE_DIR / "delete_handle_dupes_cursor.json"

_VERIFY = """
query MotoxDupeVerify($dup: ID!, $base: ID!) {
  dup: product(id: $dup) {
    id
    handle
    title
    status
    variants(first: 20) { nodes { sku } }
  }
  base: product(id: $base) {
    id
    handle
    title
    status
    variants(first: 20) { nodes { sku } }
  }
}
"""

_DELETE = """
mutation MotoxProductDelete($input: ProductDeleteInput!) {
  productDelete(input: $input) {
    deletedProductId
    userErrors { field message }
  }
}
"""


def _skus_by_handle(path: Path) -> dict[str, set[str]]:
    gid_handle: dict[str, str] = {}
    gid_skus: dict[str, set[str]] = defaultdict(set)
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            parent = row.get("__parentId")
            if parent and "sku" in row:
                sku = (row.get("sku") or "").strip().upper()
                if sku:
                    gid_skus[parent].add(sku)
                continue
            gid = row.get("id") or ""
            handle = (row.get("handle") or "").strip()
            if gid.startswith("gid://shopify/Product/") and handle:
                gid_handle[gid] = handle
    return {handle: gid_skus.get(gid, set()) for gid, handle in gid_handle.items()}


def _candidates(products_index: dict, skus_by_handle: dict[str, set[str]]) -> list[dict]:
    handles = set(products_index.keys())
    out: list[dict] = []
    for handle, meta in products_index.items():
        match = _HANDLE_DUPE.match(handle or "")
        if not match:
            continue
        base = match.group(1)
        if base not in handles:
            continue
        dup_skus = skus_by_handle.get(handle) or set()
        base_skus = skus_by_handle.get(base) or set()
        shared = sorted(dup_skus & base_skus)
        if not shared:
            continue
        base_meta = products_index.get(base) or {}
        out.append(
            {
                "handle": handle,
                "id": (meta.get("id") or "").strip(),
                "title": (meta.get("title") or "").strip(),
                "status": (meta.get("status") or "").upper(),
                "base_handle": base,
                "base_id": (base_meta.get("id") or "").strip(),
                "base_title": (base_meta.get("title") or "").strip(),
                "sku": shared[0],
                "shared_skus": shared,
            }
        )
    out.sort(key=lambda row: row["handle"])
    return out


def _sku_set(product: dict | None) -> set[str]:
    if not product:
        return set()
    nodes = ((product.get("variants") or {}).get("nodes")) or []
    return {(n.get("sku") or "").strip().upper() for n in nodes if (n.get("sku") or "").strip()}


def _admin_url(product_id: str) -> str:
    return f"https://admin.shopify.com/store/{_SHOP}/products/{product_id}"


def _verify(admin: MotoxAdmin, row: dict) -> tuple[dict | None, str]:
    body = admin.gql(
        _VERIFY,
        {
            "dup": f"gid://shopify/Product/{row['id']}",
            "base": f"gid://shopify/Product/{row['base_id']}",
        },
    )
    if body.get("errors"):
        return None, str(body.get("errors"))[:300]
    data = body.get("data") or {}
    dup = data.get("dup")
    base = data.get("base")
    if not dup:
        return None, "duplicaat bestaat niet meer"
    if not base:
        return None, "basisproduct ontbreekt"
    if (dup.get("handle") or "") != row["handle"]:
        return None, f"handle is nu {dup.get('handle')}"
    if (base.get("handle") or "") != row["base_handle"]:
        return None, f"basis-handle is nu {base.get('handle')}"
    shared = _sku_set(dup) & _sku_set(base)
    if not shared:
        return None, "geen gedeelde SKU"
    checked = dict(row)
    checked["title"] = (dup.get("title") or row["title"]).strip()
    checked["base_title"] = (base.get("title") or row["base_title"]).strip()
    checked["sku"] = sorted(shared)[0]
    checked["shared_skus"] = sorted(shared)
    return checked, ""


def _cursor_from_reports() -> str:
    """Laatste geslaagde apply-run, voor als deze wijziging nog geen cursorbestand had."""
    report_dir = OUTPUT_ROOT / "sync"
    if not report_dir.is_dir():
        return ""
    best = ""
    for path in sorted(report_dir.glob("delete_handle_dupes_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if data.get("dry_run") or data.get("errors"):
            continue
        handle = (data.get("next_start_after") or "").strip()
        if handle:
            best = handle
    return best


def _load_cursor() -> str:
    if _CURSOR_FILE.is_file():
        try:
            data = json.loads(_CURSOR_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return _cursor_from_reports()
        handle = (data.get("handle") or "").strip()
        if handle:
            return handle
    return _cursor_from_reports()


def _save_cursor(handle: str) -> None:
    _CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"handle": handle, "updated_at": int(time.time())}
    tmp = _CURSOR_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(_CURSOR_FILE)


def _delete(admin: MotoxAdmin, product_id: str) -> str:
    body = admin.gql(_DELETE, {"input": {"id": f"gid://shopify/Product/{product_id}"}})
    if body.get("errors"):
        return str(body.get("errors"))[:300]
    payload = ((body.get("data") or {}).get("productDelete")) or {}
    errors = payload.get("userErrors") or []
    if errors:
        return str(errors)[:300]
    deleted = payload.get("deletedProductId") or ""
    if not deleted:
        return "geen deletedProductId"
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Verwijder Motox-producten met handle <artikelnummer>-1"
    )
    ap.add_argument("--apply", action="store_true", help="Echt verwijderen")
    ap.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max te verwijderen producten (default 5, 0 = alle)",
    )
    ap.add_argument(
        "--start-after",
        default="",
        help="Sla handles over tot en met deze handle (overschrijft de opgeslagen cursor)",
    )
    ap.add_argument(
        "--from-start",
        action="store_true",
        help="Negeer de cursor en begin opnieuw bij de eerste handle",
    )
    ap.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Haal eerst een verse Motox-bulk op (nodig in Actions)",
    )
    args = ap.parse_args()
    dry_run = not args.apply

    if args.refresh_cache:
        refresh_motox_cache(force=True)

    if not BULK_JSONL_FILE.is_file():
        raise SystemExit(f"Geen bulk-cache: {BULK_JSONL_FILE}")

    idx = load_motox_indexes()
    products_index = idx.get("products_index") or {}
    skus_by_handle = _skus_by_handle(BULK_JSONL_FILE)
    all_rows = _candidates(products_index, skus_by_handle)
    if args.from_start:
        start_after = ""
    elif args.start_after:
        start_after = args.start_after.strip()
    else:
        start_after = _load_cursor()
    pending = [row for row in all_rows if row["handle"] > start_after] if start_after else all_rows
    skipped_local = len(all_rows) - len(pending)

    if start_after:
        log.info(
            "Hervat na %s: %s handles lokaal overgeslagen, %s nog te controleren (dry_run=%s, limit=%s)",
            start_after,
            skipped_local,
            len(pending),
            dry_run,
            args.limit,
        )
    else:
        log.info(
            "Kandidaten met gedeelde SKU: %s (dry_run=%s, limit=%s)",
            len(pending),
            dry_run,
            args.limit,
        )

    admin = MotoxAdmin()
    chosen: list[dict] = []
    skipped: list[dict] = []
    target = args.limit if args.limit > 0 else len(pending)
    examined = 0
    for row in pending:
        if len(chosen) >= target:
            break
        examined += 1
        checked, err = _verify(admin, row)
        if err or not checked:
            skipped.append({**row, "detail": err or "skip"})
            log.info("Skip %s: %s", row["handle"], err)
            continue
        chosen.append(checked)

    print()
    print(f"{'dry-run' if dry_run else 'verwijderen'}: {len(chosen)} producten")
    for i, row in enumerate(chosen, start=1):
        print(
            f"{i}. {row['handle']}  sku={row['sku']}\n"
            f"   duplicaat {row['id']}  {row['title']}\n"
            f"   {_admin_url(row['id'])}\n"
            f"   blijft    {row['base_handle']}  {row['base_id']}  {row['base_title']}"
        )
    if skipped:
        print(f"Overgeslagen bij controle: {len(skipped)}")
    left = max(0, len(pending) - examined)
    if left:
        print(f"Nog niet gecontroleerd door de limiet: {left}")
    print()

    report_rows: list[dict] = []
    ok_n = 0
    err_n = 0
    last_deleted = start_after
    for i, row in enumerate(chosen, start=1):
        if dry_run:
            ok_n += 1
            report_rows.append({**row, "ok": "1", "detail": "dry-run"})
            continue
        err = _delete(admin, row["id"])
        if err:
            err_n += 1
            report_rows.append({**row, "ok": "0", "detail": err})
            log.warning("[%s/%s] FAIL %s: %s", i, len(chosen), row["handle"], err)
            break
        ok_n += 1
        last_deleted = row["handle"]
        if not args.from_start:
            _save_cursor(last_deleted)
        report_rows.append({**row, "ok": "1", "detail": "deleted"})
        log.info("[%s/%s] verwijderd %s", i, len(chosen), row["handle"])
        time.sleep(0.35)

    out_dir = OUTPUT_ROOT / "sync"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    report_path = out_dir / f"delete_handle_dupes_{ts}.csv"
    fields = [
        "ok",
        "handle",
        "id",
        "sku",
        "title",
        "base_handle",
        "base_id",
        "base_title",
        "detail",
    ]
    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(report_rows)

    summary = {
        "dry_run": dry_run,
        "candidates": len(pending),
        "skipped_local": skipped_local,
        "resumed_after": start_after,
        "chosen": len(chosen),
        "skipped_live": len(skipped),
        "ok": ok_n,
        "errors": err_n,
        "remaining_after_cap": max(0, len(pending) - examined),
        "report": str(report_path),
        "next_start_after": (
            chosen[-1]["handle"] if dry_run and chosen else last_deleted
        ),
    }
    summary_path = out_dir / f"delete_handle_dupes_{ts}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 2 if err_n else 0


if __name__ == "__main__":
    raise SystemExit(main())
