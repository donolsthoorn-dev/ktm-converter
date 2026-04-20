#!/usr/bin/env python3
"""
Voeg ontbrekende XML-varianten (SKU's) toe aan **bestaande** Shopify-producten.

Bron:
  - ``output/xml_vs_shopify_variant_counts.csv`` (of ``--csv``), kolommen zoals
    ``compare_xml_variant_counts_to_supabase.py`` schrijft.
  - Huidige KTM-XML (``load_products()``) voor volledige SKU-lijst + optiewaarde
    per SKU (niet alleen de afgekapte ``skus_in_xml_not_in_mirror``-kolom).

Voor elk CSV-product met ``shopify_product_id`` en ``delta`` > 0:
  - Haal live Shopify-product op (opties + bestaande variant-SKU's).
  - Bepaal ontbrekende SKU's t.o.v. XML voor dezelfde handle.
  - Alleen ondersteund als het product **precies één** optie-dimensie heeft
    (zoals ``Title``, ``Size``, …). Meerdere opties → overslaan (handmatig).

Prijzen / inventory policy: zelfde KTM-prijs-CSV-merge als
``shopify_sync_from_pricelist_csv.py`` (optioneel ``--csv`` prijsbestanden).

Standaard **dry-run** (geen mutaties). Gebruik ``--apply`` om
``productVariantsBulkCreate`` uit te voeren.

Daarna: ``python3 scripts/shopify_refresh_variant_cache.py`` en eventueel
``shopify_sync_from_pricelist_csv.py``.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
from datetime import date
from decimal import Decimal, InvalidOperation
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

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

from modules.pricing_loader import load_price_index, normalize_sku_key  # noqa: E402
from modules.xml_loader import load_products, normalize_shopify_product_handle  # noqa: E402


def _load_sync_module():
    path = ROOT / "scripts" / "shopify_sync_from_pricelist_csv.py"
    spec = importlib.util.spec_from_file_location("shopify_sync_from_pricelist_csv", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Kan module niet laden: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _product_gid(numeric_id: str) -> str:
    return f"gid://shopify/Product/{int(numeric_id)}"


def _graphql_product_variant_context(
    sync: Any,
    shop: str,
    token: str,
    api_ver: str,
    product_id_numeric: str,
    sess: requests.Session,
) -> dict[str, Any] | None:
    q = """
query KtmProductVariantContext($id: ID!) {
  product(id: $id) {
    id
    handle
    hasOnlyDefaultVariant
    options {
      id
      name
      position
      values
    }
    variants(first: 250) {
      edges {
        node {
          id
          sku
          selectedOptions { name value }
        }
      }
    }
  }
}
"""
    data = sync.graphql_post(
        shop,
        token,
        api_ver,
        q,
        {"id": _product_gid(product_id_numeric)},
        sess=sess,
    )
    return (data or {}).get("product")


def _parse_price_to_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(Decimal(str(raw).replace(",", ".")))
    except (InvalidOperation, ValueError):
        return None


def _inventory_policy_for_create(desired: dict[str, Any] | None) -> str | None:
    if not desired:
        return "CONTINUE"
    pol = str(desired.get("inventory_policy") or "").strip().upper()
    if pol == "DENY":
        return "DENY"
    if pol == "CONTINUE":
        return "CONTINUE"
    return None


def _read_diff_csv(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with open(path, encoding="utf-8", newline="") as fh:
        r = csv.DictReader(fh)
        for row in r:
            rows.append({k: (v or "").strip() for k, v in row.items()})
    return rows


def _build_xml_indexes() -> tuple[
    dict[str, set[str]],
    dict[str, dict[str, dict[str, str]]],
]:
    """
    Returns:
      skus_by_handle: handle -> set of skus
      detail: handle -> sku -> { variant, variant_label, title }
    """
    products = load_products()
    skus_by_handle: dict[str, set[str]] = {}
    detail: dict[str, dict[str, dict[str, str]]] = {}
    for p in products:
        h = normalize_shopify_product_handle(str(p.get("handle") or ""))
        sku = normalize_sku_key(p.get("sku"))
        if not h or not sku:
            continue
        skus_by_handle.setdefault(h, set()).add(sku)
        d = detail.setdefault(h, {})
        if sku not in d:
            d[sku] = {
                "variant": str(p.get("variant") or "").strip() or "Default Title",
                "variant_label": str(p.get("variant_label") or "").strip() or "Title",
                "title": str(p.get("title") or "").strip(),
            }
    return skus_by_handle, detail


def main() -> int:
    p = argparse.ArgumentParser(
        description="Ontbrekende XML-varianten toevoegen aan bestaande Shopify-producten "
        "(op basis van xml_vs_shopify_variant_counts.csv + live product)."
    )
    p.add_argument(
        "--diff-csv",
        type=Path,
        default=ROOT / "output" / "xml_vs_shopify_variant_counts.csv",
        help="CSV van compare_xml_variant_counts_to_supabase",
    )
    p.add_argument(
        "--csv",
        action="append",
        dest="price_csv_paths",
        metavar="PAD",
        help="KTM-prijs-CSV (herhaalbaar); default = zelfde als shopify_sync_from_pricelist_csv",
    )
    p.add_argument(
        "--limit-products",
        type=int,
        default=0,
        metavar="N",
        help="Max. aantal producten uit diff-CSV verwerken (0 = alles)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Voer productVariantsBulkCreate uit (zonder vlag: alleen dry-run met live reads)",
    )
    p.add_argument(
        "--chunk",
        type=int,
        default=25,
        help="Max. varianten per productVariantsBulkCreate-call",
    )
    args = p.parse_args()

    sync = _load_sync_module()
    sync.configure_graphql_inflight(max(1, min(int(os.environ.get("SHOPIFY_GRAPHQL_INFLIGHT", "4")), 8)))

    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
    api_ver = (os.environ.get("SHOPIFY_ADMIN_API_VERSION") or "").strip() or "2024-10"

    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt.", file=sys.stderr)
        return 1

    diff_path = args.diff_csv.resolve()
    if not diff_path.is_file():
        print(f"Diff-CSV ontbreekt: {diff_path}", file=sys.stderr)
        return 1

    print("XML laden (handles + optiewaarden per SKU)…", flush=True)
    xml_skus_by_handle, xml_detail_by_handle = _build_xml_indexes()

    print("Prijzen/barcodes (0150) laden…", flush=True)
    try:
        price_index, barcode_index, _status_index = load_price_index()
    except (FileNotFoundError, OSError, RuntimeError) as e:
        print(f"Waarschuwing: prijsbestand niet geladen — {e}", file=sys.stderr)
        price_index, barcode_index = {}, {}

    today = date.today()
    price_csv_paths = sync.resolve_csv_paths(args.price_csv_paths)
    desired_by_sku = sync.read_pricelist_csv_desired_many(price_csv_paths, today)
    print(f"Prijs-CSV: {len(price_csv_paths)} bestand(en), {len(desired_by_sku)} SKU-regels", flush=True)

    diff_rows = _read_diff_csv(diff_path)
    sess = requests.Session()
    sess.trust_env = False

    mutation = """
mutation KtmAddVariants(
  $productId: ID!,
  $variants: [ProductVariantsBulkInput!]!,
  $strategy: ProductVariantsBulkCreateStrategy
) {
  productVariantsBulkCreate(
    productId: $productId,
    variants: $variants,
    strategy: $strategy
  ) {
    productVariants { id sku title }
    userErrors { field message }
  }
}
"""

    stats = {
        "csv_rows": len(diff_rows),
        "products_considered": 0,
        "products_skipped_multi_option": 0,
        "products_skipped_no_missing": 0,
        "products_skipped_no_price": 0,
        "variants_would_create": 0,
        "variants_created": 0,
        "graphql_errors": 0,
    }
    log_apply: list[dict[str, Any]] = []

    n_done_products = 0
    for row in diff_rows:
        if args.limit_products and n_done_products >= args.limit_products:
            break
        note = row.get("note") or ""
        if note:
            continue
        pid_raw = row.get("shopify_product_id") or ""
        if not pid_raw:
            continue
        try:
            delta = int(row.get("delta") or "0")
        except ValueError:
            continue
        if delta <= 0:
            continue

        handle_csv = normalize_shopify_product_handle(row.get("handle") or "")
        if not handle_csv:
            continue

        stats["products_considered"] += 1
        n_done_products += 1

        prod = _graphql_product_variant_context(sync, shop, token, api_ver, pid_raw, sess)
        if not prod:
            print(f"[{handle_csv}] product id {pid_raw}: niet gevonden in Shopify", flush=True)
            stats["graphql_errors"] += 1
            continue

        handle_shop = normalize_shopify_product_handle(str(prod.get("handle") or ""))
        if handle_shop != handle_csv:
            print(
                f"[{handle_csv}] waarschuwing: CSV-handle ≠ Shopify-handle ({handle_shop}); "
                f"ga door op product_id {pid_raw}",
                flush=True,
            )

        options = prod.get("options") or []
        if len(options) != 1:
            print(
                f"[{handle_csv}] overslaan: {len(options)} optie(s); alleen 1 optie wordt ondersteund.",
                flush=True,
            )
            stats["products_skipped_multi_option"] += 1
            continue

        opt_name = str((options[0] or {}).get("name") or "Title").strip() or "Title"

        existing_skus: set[str] = set()
        existing_values: set[str] = set()
        for edge in (prod.get("variants") or {}).get("edges") or []:
            node = (edge or {}).get("node") or {}
            s = normalize_sku_key(node.get("sku"))
            if s:
                existing_skus.add(s)
            for so in node.get("selectedOptions") or []:
                if str(so.get("name") or "") == opt_name:
                    existing_values.add(str(so.get("value") or "").strip())

        xml_skus = xml_skus_by_handle.get(handle_csv) or xml_skus_by_handle.get(handle_shop)
        if not xml_skus:
            print(f"[{handle_csv}] geen XML-skus voor deze handle — overslaan", flush=True)
            continue

        missing = sorted(xml_skus - existing_skus)
        if not missing:
            stats["products_skipped_no_missing"] += 1
            continue

        variants_payload: list[dict[str, Any]] = []
        used_values: set[str] = set(existing_values)

        for sku in missing:
            meta = (xml_detail_by_handle.get(handle_csv) or xml_detail_by_handle.get(handle_shop) or {}).get(
                sku
            ) or {}
            display_value = str(meta.get("variant") or "").strip() or sku
            # Meerdere ontbrekende regels met "Default Title" zouden anders botsen.
            if display_value in ("Default Title", "Title", ""):
                display_value = sku
            if display_value in used_values:
                display_value = sku
            if display_value in used_values:
                print(
                    f"[{handle_csv}] SKU {sku}: geen unieke optiewaarde (botsing) — overslaan.",
                    flush=True,
                )
                continue
            used_values.add(display_value)

            want = desired_by_sku.get(sku) or {}
            price_raw = want.get("price_incl")
            price_f = _parse_price_to_float(str(price_raw) if price_raw is not None else "")
            if price_f is None or price_f <= 0:
                p_idx = price_index.get(sku) or price_index.get(normalize_sku_key(sku))
                price_f = _parse_price_to_float(str(p_idx) if p_idx else "")
            if price_f is None or price_f <= 0:
                print(f"[{handle_csv}] SKU {sku}: geen prijs in CSV/0150 — overslaan", flush=True)
                continue

            inv = _inventory_policy_for_create(want)
            entry: dict[str, Any] = {
                "sku": sku,
                "price": price_f,
                "optionValues": [{"name": display_value, "optionName": opt_name}],
            }
            bc = (barcode_index.get(sku) or barcode_index.get(normalize_sku_key(sku)) or "").strip()
            if bc:
                entry["barcode"] = bc
            if inv:
                entry["inventoryPolicy"] = inv
            variants_payload.append(entry)

        if not variants_payload:
            stats["products_skipped_no_price"] += 1
            continue

        stats["variants_would_create"] += len(variants_payload)

        if not args.apply:
            print(
                f"[dry-run] {handle_csv} pid={pid_raw} optie={opt_name!r} "
                f"→ {len(variants_payload)} variant(en): "
                f"{', '.join(v['sku'] for v in variants_payload[:12])}"
                f"{' …' if len(variants_payload) > 12 else ''}",
                flush=True,
            )
            continue

        pid_gid = _product_gid(pid_raw)
        strategy = "DEFAULT"
        created_here = 0
        for i in range(0, len(variants_payload), max(1, args.chunk)):
            chunk = variants_payload[i : i + max(1, args.chunk)]
            data = sync.graphql_post(
                shop,
                token,
                api_ver,
                mutation,
                {"productId": pid_gid, "variants": chunk, "strategy": strategy},
                sess=sess,
            )
            payload = (data or {}).get("productVariantsBulkCreate") or {}
            uerr = payload.get("userErrors") or []
            if uerr:
                stats["graphql_errors"] += 1
                print(
                    f"[{handle_csv}] userErrors chunk {i}: {json.dumps(uerr, ensure_ascii=False)[:2000]}",
                    file=sys.stderr,
                    flush=True,
                )
                break
            pvs = payload.get("productVariants") or []
            created_here += len(pvs)
            for pv in pvs:
                log_apply.append(
                    {
                        "handle": handle_csv,
                        "shopify_product_id": pid_raw,
                        "variant_gid": pv.get("id"),
                        "sku": pv.get("sku"),
                    }
                )
        stats["variants_created"] += created_here
        print(
            f"[apply] {handle_csv} pid={pid_raw}: {created_here}/{len(variants_payload)} varianten aangemaakt.",
            flush=True,
        )

    print(json.dumps(stats, indent=2), flush=True)
    if args.apply and log_apply:
        log_path = ROOT / "output" / "logs" / f"shopify_add_xml_variants_{date.today().isoformat()}.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(json.dumps(log_apply, indent=2), encoding="utf-8")
        print(f"Log geschreven: {log_path}", flush=True)

    if not args.apply:
        print(
            "\nDit was dry-run (geen writes). Start met --apply om te muteren; daarna "
            "shopify_refresh_variant_cache.py + shopify_sync_from_pricelist_csv.py.",
            flush=True,
        )
    else:
        print(
            "\nKlaar met mutaties. Draai: python3 scripts/shopify_refresh_variant_cache.py",
            flush=True,
        )
    if args.apply:
        return 0 if stats["graphql_errors"] == 0 else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
