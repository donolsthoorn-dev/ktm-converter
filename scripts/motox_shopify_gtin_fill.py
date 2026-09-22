#!/usr/bin/env python3
"""
Vul lege Motox-barcodes vanuit e-bihr BarCode (EAN/GTIN).

Alleen fill-if-empty: een barcode die al staat wordt niet overschreven.
Nieuwe producten krijgen de code al bij het aanmaken; deze job is de inhaal
voor varianten die toen leeg waren of waar Bihr de code later toevoegde.

  python3 scripts/motox_shopify_gtin_fill.py --raw-dir motox/e-bihr/raw/<ts>
  python3 scripts/motox_shopify_gtin_fill.py --fetch
  python3 scripts/motox_shopify_gtin_fill.py --fetch --apply --limit 200

Vereist: SHOPIFY_MOTOX_ACCESS_TOKEN, SHOPIFY_MOTOX_SHOP_DOMAIN.
Met --fetch ook BIHR_USERNAME en BIHR_PASSWORD.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.build_products import _barcode, _part_number, load_v3_products  # noqa: E402
from modules.ebihr.client import BihrClient  # noqa: E402
from modules.ebihr.constants import RAW_ROOT  # noqa: E402
from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402
from modules.pricing_loader import normalize_gtin  # noqa: E402

_PRODUCTS = """
query MotoxGtinProducts($c: String) {
  products(first: 50, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      status
      variants(first: 100) {
        nodes { id sku barcode }
      }
    }
  }
}
"""


def _barcode_index(raw_dir: Path) -> dict[str, str]:
    index: dict[str, str] = {}
    for product in load_v3_products(raw_dir):
        sku = _part_number(product).strip().upper()
        code = normalize_gtin(_barcode(product))
        if sku and code:
            index[sku] = code
    return index


def _gid_num(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Schrijf naar Motox")
    ap.add_argument("--fetch", action="store_true", help="Haal het Bihr Products-catalogusbestand op")
    ap.add_argument("--raw-dir", type=Path, default=None, help="Bestaande raw-map met v3_Products")
    ap.add_argument("--limit", type=int, default=0, help="Max te vullen varianten (0 = alle)")
    ap.add_argument("--sleep", type=float, default=0.15, help="Pauze tussen product-writes")
    args = ap.parse_args()

    raw_dir = args.raw_dir
    if args.fetch:
        raw_dir = raw_dir or (RAW_ROOT / f"gtin-{int(time.time())}")
        print(f"Bihr Products ophalen naar {raw_dir}", flush=True)
        BihrClient().fetch_catalog("Products", raw_dir)
    if raw_dir is None or not raw_dir.is_dir():
        raise SystemExit("Geen raw-dir. Gebruik --fetch of --raw-dir.")

    print("BarCode-index laden…", flush=True)
    barcodes = _barcode_index(raw_dir)
    print(f"{len(barcodes)} geldige EAN/GTIN-codes in e-bihr.", flush=True)
    if not barcodes:
        raise SystemExit("Geen BarCode in het Products-bestand.")

    admin = MotoxAdmin()
    by_product: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
    scanned_products = 0
    scanned_variants = 0
    already = 0
    no_source = 0
    skipped_archived = 0
    cursor = None
    print(f"Shop: {admin.domain}  apply={args.apply}", flush=True)

    while True:
        body = admin.gql(_PRODUCTS, {"c": cursor})
        if body.get("errors"):
            raise SystemExit(str(body.get("errors"))[:500])
        conn = ((body.get("data") or {}).get("products")) or {}
        for product in conn.get("nodes") or []:
            scanned_products += 1
            status = (product.get("status") or "").upper()
            if status == "ARCHIVED":
                skipped_archived += 1
                continue
            pid = _gid_num(product.get("id") or "")
            handle = product.get("handle") or ""
            for variant in ((product.get("variants") or {}).get("nodes")) or []:
                scanned_variants += 1
                current = str(variant.get("barcode") or "").strip()
                if current:
                    already += 1
                    continue
                sku = str(variant.get("sku") or "").strip().upper()
                code = barcodes.get(sku) or ""
                if not code:
                    no_source += 1
                    continue
                vid = _gid_num(variant.get("id") or "")
                by_product[pid].append((vid, code, sku, handle))
                if args.limit and sum(len(v) for v in by_product.values()) >= args.limit:
                    break
            if args.limit and sum(len(v) for v in by_product.values()) >= args.limit:
                break
        if args.limit and sum(len(v) for v in by_product.values()) >= args.limit:
            break
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.05)

    todo = sum(len(v) for v in by_product.values())
    print(
        f"Gescand: {scanned_products} producten, {scanned_variants} varianten. "
        f"Archief overgeslagen: {skipped_archived}. Al barcode: {already}. "
        f"Geen BarCode bij Bihr: {no_source}. Te vullen: {todo}.",
        flush=True,
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"motox_shopify_gtin_fill_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["product_id", "variant_id", "handle", "sku", "gtin", "result"]
    ok = fail = 0
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        if not args.apply:
            for pid, items in by_product.items():
                for vid, gtin, sku, handle in items:
                    writer.writerow(
                        {
                            "product_id": pid,
                            "variant_id": vid,
                            "handle": handle,
                            "sku": sku,
                            "gtin": gtin,
                            "result": "dry-run",
                        }
                    )
                    ok += 1
            print(f"Dry-run CSV: {out} ({ok} rijen). Geen Shopify-writes.", flush=True)
            return 0

        done = 0
        for pid, items in by_product.items():
            for start in range(0, len(items), 100):
                chunk = items[start : start + 100]
                err = admin.update_variant_barcodes(
                    pid,
                    [(vid, gtin) for vid, gtin, _sku, _handle in chunk],
                )
                result = "ok" if not err else err
                if err:
                    fail += len(chunk)
                    print(f"Fout product {pid}: {err[:300]}", flush=True)
                else:
                    ok += len(chunk)
                for vid, gtin, sku, handle in chunk:
                    writer.writerow(
                        {
                            "product_id": pid,
                            "variant_id": vid,
                            "handle": handle,
                            "sku": sku,
                            "gtin": gtin,
                            "result": result,
                        }
                    )
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(by_product)} producten  ok={ok} fail={fail}", flush=True)
            time.sleep(max(0.0, args.sleep))

    print(f"Klaar. ok={ok} fail={fail}  CSV: {out}", flush=True)
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
