#!/usr/bin/env python3
"""
Zet de KTM-verkoopprijs op Motox-producten van KTM, Husqvarna, GasGas en WP
die niet op de Online Store staan (Synkro/POS).

De prijs is dezelfde berekening als ktm_price_eta_status_sync: CSV incl. btw,
plus 9% waar het producttype dat krijgt. Gepubliceerde producten blijven
staan, zodat de e-bihr-webshop zijn eigen prijs houdt. Voorraad en status
worden niet aangeraakt.

  python3 scripts/motox_oem_price_sync.py
  python3 scripts/motox_oem_price_sync.py --apply --limit 50

Vereist prijs-CSV's in input/ (zoals de KTM-prijsjob) en
SHOPIFY_MOTOX_ACCESS_TOKEN, SHOPIFY_MOTOX_SHOP_DOMAIN.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
os.chdir(ROOT)

from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402
from modules.price_markup import apply_price_markup_str  # noqa: E402
import shopify_sync_from_pricelist_csv as sync  # noqa: E402

OEM_VENDORS = frozenset({"ktm", "husqvarna", "gasgas", "wp"})
_VENDOR_QUERY = ("KTM", "Husqvarna", "GasGas", "WP")

_PRODUCTS = """
query MotoxOemPrices($c: String, $q: String!) {
  products(first: 50, query: $q, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      vendor
      productType
      status
      publishedAt
      variants(first: 100) {
        nodes { id sku price }
      }
    }
  }
}
"""


def _vendor_key(vendor: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (vendor or "").lower())


def _cents(value: str | None) -> Decimal | None:
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        return None
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except Exception:
        return None


def _gid_num(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def _desired_prices() -> dict[str, str]:
    paths = sync.resolve_csv_paths(None)
    desired = sync.read_pricelist_csv_desired_many(paths, date.today())
    out: dict[str, str] = {}
    for sku, row in desired.items():
        price = row.get("price_incl")
        if price:
            out[str(sku).strip().upper()] = str(price)
    print(f"{len(out)} verkoopprijzen uit {len(paths)} prijs-CSV's.", flush=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Schrijf naar Motox")
    ap.add_argument("--limit", type=int, default=0, help="Max prijswijzigingen (0 = alle)")
    ap.add_argument("--sleep", type=float, default=0.15, help="Pauze tussen product-writes")
    args = ap.parse_args()

    prices = _desired_prices()
    if not prices:
        raise SystemExit("Geen prijzen in de CSV's.")

    admin = MotoxAdmin()
    print(f"Shop: {admin.domain}  apply={args.apply}", flush=True)

    # product_id -> list of (variant_id, sku, handle, old, new)
    changes: dict[str, list[tuple[str, str, str, str, str]]] = defaultdict(list)
    scanned = 0
    skipped_published = 0
    skipped_vendor = 0
    unchanged = 0
    no_price = 0
    seen_ids: set[str] = set()

    for vendor_name in _VENDOR_QUERY:
        cursor = None
        query = f'vendor:"{vendor_name}"'
        while True:
            body = admin.gql(_PRODUCTS, {"c": cursor, "q": query})
            if body.get("errors"):
                raise SystemExit(str(body.get("errors"))[:500])
            conn = ((body.get("data") or {}).get("products")) or {}
            for product in conn.get("nodes") or []:
                pid = _gid_num(product.get("id") or "")
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                scanned += 1
                if _vendor_key(product.get("vendor") or "") not in OEM_VENDORS:
                    skipped_vendor += 1
                    continue
                if (product.get("status") or "").upper() == "ARCHIVED":
                    continue
                if product.get("publishedAt"):
                    skipped_published += 1
                    continue
                handle = product.get("handle") or ""
                product_type = product.get("productType") or ""
                for variant in ((product.get("variants") or {}).get("nodes")) or []:
                    sku = str(variant.get("sku") or "").strip().upper()
                    raw = prices.get(sku)
                    if not sku or not raw:
                        no_price += 1
                        continue
                    want = apply_price_markup_str(raw, product_type) or raw
                    want_d = _cents(want)
                    have_d = _cents(variant.get("price"))
                    if want_d is None or want_d <= 0:
                        no_price += 1
                        continue
                    if have_d == want_d:
                        unchanged += 1
                        continue
                    vid = _gid_num(variant.get("id") or "")
                    changes[pid].append((vid, sku, handle, f"{have_d:.2f}" if have_d is not None else "", f"{want_d:.2f}"))
                    if args.limit and sum(len(v) for v in changes.values()) >= args.limit:
                        break
                if args.limit and sum(len(v) for v in changes.values()) >= args.limit:
                    break
            if args.limit and sum(len(v) for v in changes.values()) >= args.limit:
                break
            page = conn.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
            time.sleep(0.05)
        if args.limit and sum(len(v) for v in changes.values()) >= args.limit:
            break

    todo = sum(len(v) for v in changes.values())
    print(
        f"OEM-producten gezien: {scanned}. Andere vendor: {skipped_vendor}. "
        f"Gepubliceerd overgeslagen: {skipped_published}. "
        f"Prijs gelijk: {unchanged}. Geen CSV-prijs: {no_price}. Te wijzigen: {todo}.",
        flush=True,
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"motox_oem_price_sync_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["product_id", "variant_id", "handle", "sku", "old_price", "new_price", "result"]
    ok = fail = 0
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        if not args.apply:
            for pid, items in changes.items():
                for vid, sku, handle, old, new in items:
                    writer.writerow(
                        {
                            "product_id": pid,
                            "variant_id": vid,
                            "handle": handle,
                            "sku": sku,
                            "old_price": old,
                            "new_price": new,
                            "result": "dry-run",
                        }
                    )
                    ok += 1
            print(f"Dry-run CSV: {out} ({ok} rijen). Geen Shopify-writes.", flush=True)
            return 0

        done = 0
        for pid, items in changes.items():
            err = admin.update_variant_prices(
                pid,
                [(vid, new) for vid, _sku, _handle, _old, new in items],
            )
            result = "ok" if not err else err
            if err:
                fail += len(items)
                print(f"Fout product {pid}: {err[:300]}", flush=True)
            else:
                ok += len(items)
            for vid, sku, handle, old, new in items:
                writer.writerow(
                    {
                        "product_id": pid,
                        "variant_id": vid,
                        "handle": handle,
                        "sku": sku,
                        "old_price": old,
                        "new_price": new,
                        "result": result,
                    }
                )
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(changes)} producten  ok={ok} fail={fail}", flush=True)
            time.sleep(max(0.0, args.sleep))

    print(f"Klaar. ok={ok} fail={fail}  CSV: {out}", flush=True)
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
