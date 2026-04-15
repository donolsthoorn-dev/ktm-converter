#!/usr/bin/env python3
"""
Eenmalig: vul cache/shopify_pricelist_sync_state.json vanuit een **Shopify Admin product-export** (CSV).

Dit is wat je bedoelde als “basis uit Shopify”: prijs en productstatus zoals ze nu in de shop staan,
zonder alles eerst via de sync naar Shopify te duwen. Daarna doet shopify_sync_from_pricelist_csv.py
alleen nog delta’s t.o.v. je KTM prijs-CSV’s.

Wat zit **niet** in een standaard product-CSV:
  - Geen variant-numerieke ID’s → de **variant-cache** (shopify_eta_sync_sku_variant.json) blijft
    uit:  python3 scripts/shopify_refresh_variant_cache.py
  - Geen ETA-metafield → optioneel **--merge-eta-from-pricelist-csv** om hqETADate uit een
    KTM-export in state te zetten (oude vlag **--merge-eta-from-0150** werkt nog).

Kolomnamen (komma-gescheiden) zoals Shopify exporteert o.a.:
  Handle, …, Variant SKU, Variant Price, Status (active/draft/archived; vaak alleen op eerste rij per product).

Workflow:
  1) In Shopify: Products → Export (soms 2+ bestanden bij grote catalogus).
  2) python3 scripts/shopify_refresh_variant_cache.py
  3) python3 scripts/bootstrap_state_from_shopify_export.py \\
       --shopify-csv export1.csv --shopify-csv export2.csv \\
       --merge-eta-from-pricelist-csv input/0150_35_Z1_EUR_EN_csv.csv
  4) python3 scripts/shopify_sync_from_pricelist_csv.py

Meerdere exports: zelfde optie herhalen; bij dezelfde SKU wint het **laatste** bestand.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_sync_module():
    path = PROJECT_ROOT / "scripts" / "shopify_sync_from_pricelist_csv.py"
    spec = importlib.util.spec_from_file_location("shopify_sync_from_pricelist_csv", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Kan module niet laden: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _norm_price(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip().strip('"')
    if not s:
        return None
    try:
        v = float(s.replace(",", "."))
        return f"{v:.2f}"
    except ValueError:
        return None


def _map_shopify_status(raw: str | None) -> str:
    s = (raw or "").strip().lower()
    if s == "draft":
        return "DRAFT"
    return "ACTIVE"


def read_shopify_products_csv(path: Path) -> dict[str, dict]:
    """
    SKU (uppercase) -> { price_incl, product_status }.
    Status wordt per Handle doorgegeven als alleen de eerste variantrij die kolom vult.
    """
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin1")
    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc) as fh:
                reader = csv.DictReader(fh)
                if not reader.fieldnames:
                    return {}
                fields = {n.strip(): n for n in reader.fieldnames if n}

                def col(*names: str) -> str | None:
                    for n in names:
                        for k, v in fields.items():
                            if k.lower().replace(" ", "") == n.lower().replace(" ", ""):
                                return v
                            if k.lower() == n.lower():
                                return v
                    return None

                c_sku = col("Variant SKU", "SKU", "Variant SKU")
                c_price = col("Variant Price", "Price")
                c_status = col("Status")
                c_handle = col("Handle")
                if not c_sku or not c_price:
                    raise ValueError(
                        f"Verwacht kolommen Variant SKU en Variant Price; gevonden: {reader.fieldnames}"
                    )

                out: dict[str, dict] = {}
                last_status_by_handle: dict[str, str] = {}

                for row in reader:
                    sku = (row.get(c_sku) or "").strip()
                    if not sku:
                        continue
                    sku_u = sku.upper()

                    handle = (row.get(c_handle) or "").strip() if c_handle else ""
                    status_cell = (row.get(c_status) or "").strip() if c_status else ""
                    if handle and status_cell:
                        last_status_by_handle[handle] = status_cell

                    effective_status = status_cell or (last_status_by_handle.get(handle, "") if handle else "")
                    product_status = _map_shopify_status(effective_status)

                    price_incl = _norm_price(row.get(c_price))
                    entry: dict = {"product_status": product_status}
                    if price_incl is not None:
                        entry["price_incl"] = price_incl
                    out[sku_u] = entry
                return out
        except UnicodeDecodeError:
            continue
        except OSError:
            raise
    raise OSError(f"CSV kon niet worden gelezen: {path}")


def main() -> int:
    sync = _load_sync_module()
    sync.load_dotenv()

    p = argparse.ArgumentParser(
        description="Vul shopify_pricelist_sync_state.json vanuit Shopify product-CSV export"
    )
    p.add_argument(
        "--shopify-csv",
        type=Path,
        action="append",
        required=True,
        metavar="PAD",
        help="Shopify product-export (komma-CSV). Meerdere: optie herhalen; latere file wint bij dubbele SKU.",
    )
    p.add_argument(
        "--merge-eta-from-pricelist-csv",
        type=Path,
        metavar="PAD",
        dest="merge_eta_csv",
        help="Optioneel: hqETADate uit KTM prijs-CSV (zelfde logica als sync) in state mergen",
    )
    p.add_argument(
        "--merge-eta-from-0150",
        type=Path,
        metavar="PAD",
        dest="merge_eta_csv",
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--state-file",
        type=Path,
        default=sync.DEFAULT_STATE_FILE,
        help=f"Uitvoer (default: {sync.DEFAULT_STATE_FILE})",
    )
    p.add_argument(
        "--variant-cache",
        type=Path,
        default=sync.DEFAULT_VARIANT_CACHE,
        help="Alleen SKU's die in deze cache staan (default aan)",
    )
    p.add_argument(
        "--all-export-skus",
        action="store_true",
        help="Alle SKU's uit de Shopify-export in state; default: alleen overlap met variant-cache",
    )
    args = p.parse_args()

    from_shop: dict[str, dict] = {}
    for raw_path in args.shopify_csv:
        shopify_path = raw_path.resolve()
        if not shopify_path.is_file():
            print(f"Bestand niet gevonden: {shopify_path}", flush=True)
            return 1
        try:
            part = read_shopify_products_csv(shopify_path)
        except ValueError as e:
            print(f"{shopify_path}: {e}", flush=True)
            return 1
        overlap = len(set(from_shop) & set(part))
        from_shop.update(part)
        print(
            f"  + {shopify_path.name}: {len(part)} SKU's "
            f"({overlap} overlap met eerdere bestanden → overschreven)",
            flush=True,
        )

    today = date.today()
    eta_by_sku: dict | None = None
    if args.merge_eta_csv:
        eta_path = args.merge_eta_csv.resolve()
        if not eta_path.is_file():
            print(f"Prijs-CSV niet gevonden: {eta_path}", flush=True)
            return 1
        eta_by_sku = sync.read_pricelist_csv_desired(eta_path, today)

    only_cache = not args.all_export_skus
    sku_to_vp: dict = {}
    if only_cache:
        cache_path = args.variant_cache.resolve()
        if not cache_path.is_file():
            print(
                f"Variant-cache ontbreekt: {cache_path}\n"
                "  python3 scripts/shopify_refresh_variant_cache.py\n"
                "Of gebruik --all-export-skus.",
                flush=True,
            )
            return 1
        sku_to_vp = sync.load_variant_cache(cache_path)

    state: dict = {}
    skipped = 0
    for sku, entry in from_shop.items():
        if only_cache and sku not in sku_to_vp:
            skipped += 1
            continue
        row = dict(entry)
        if eta_by_sku is not None:
            row["eta_iso"] = eta_by_sku[sku]["eta_iso"] if sku in eta_by_sku else None
        else:
            row["eta_iso"] = None
        state[sku] = row

    out = args.state_file.resolve()
    sync.save_state(out, state)

    print(
        f"Shopify-export(s) samen: {len(from_shop)} unieke SKU's\n"
        f"  State-regels geschreven: {len(state)}\n"
        f"  Overgeslagen (niet in variant-cache): {skipped if only_cache else 0}\n"
        f"  ETA uit prijs-CSV gemerged: "
        f"{'ja' if eta_by_sku else 'nee — gebruik --merge-eta-from-pricelist-csv om ETA’s te alignen'}",
        flush=True,
    )
    print(f"Geschreven: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
