#!/usr/bin/env python3
"""
Verwijder de dubbele maat-optie op bestaande Motox-producten.

Bihr levert "Size" of "Shoes size" als variant-as en hetzelfde getal nog
eens als kenmerk ("US maat" of "Schoenmaat"). Alleen het kenmerk gaat weg,
en alleen als die variant-as er ook is.
Varianten en SKU's blijven. Bij een botsing slaat het product over.

Standaard alleen een plan. --apply schrijft via productSet.

  python3 scripts/motox_drop_duplicate_size.py 8009751
  python3 scripts/motox_drop_duplicate_size.py 8009751 --apply
  python3 scripts/motox_drop_duplicate_size.py
  python3 scripts/motox_drop_duplicate_size.py --apply

Zonder handles scant de script de hele catalogus.

Vereist: SHOPIFY_MOTOX_ACCESS_TOKEN + SHOPIFY_MOTOX_SHOP_DOMAIN in .env.motox.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.build_products import (  # noqa: E402
    SIZE_ATTRIBUTE_DUPLICATES,
    SIZE_VARIATION_NAMES,
)
from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402

_SCAN = """
query MotoxSizeScan($cursor: String) {
  products(first: 50, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      options { id name }
    }
  }
}
"""

_BY_HANDLE = """
query MotoxSizeByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    id
    handle
    title
    options { id name }
  }
}
"""

_VARIANTS = """
query MotoxSizeVariants($id: ID!, $cursor: String) {
  product(id: $id) {
    variants(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        sku
        barcode
        price
        inventoryPolicy
        selectedOptions { name value }
      }
    }
  }
}
"""

_SET = """
mutation MotoxDropSizeOption($input: ProductSetInput!, $synchronous: Boolean!) {
  productSet(input: $input, synchronous: $synchronous) {
    product { id options { name } }
    userErrors { field message code }
  }
}
"""


def _gql_data(admin: MotoxAdmin, query: str, variables: dict) -> dict:
    body = admin.gql(query, variables)
    if body.get("errors"):
        raise RuntimeError(str(body.get("errors"))[:500])
    return body.get("data") or {}


def _duplicate_options(options: list[dict]) -> list[dict]:
    names = [(opt.get("name") or "").strip() for opt in options]
    if not any(name.lower() in SIZE_VARIATION_NAMES for name in names):
        return []
    return [
        opt
        for opt, name in zip(options, names)
        if name.lower() in SIZE_ATTRIBUTE_DUPLICATES and opt.get("id")
    ]


def _variant_pages(admin: MotoxAdmin, product_id: str) -> list[dict]:
    nodes: list[dict] = []
    cursor = None
    while True:
        data = _gql_data(admin, _VARIANTS, {"id": product_id, "cursor": cursor})
        conn = ((data.get("product") or {}).get("variants")) or {}
        nodes.extend(conn.get("nodes") or [])
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage") or not page.get("endCursor"):
            return nodes
        cursor = page["endCursor"]


def _collision(variants: list[dict], drop_names: set[str]) -> str:
    seen: dict[tuple[str, ...], int] = {}
    for variant in variants:
        key = tuple(
            f"{(opt.get('name') or '').strip()}={(opt.get('value') or '').strip()}"
            for opt in (variant.get("selectedOptions") or [])
            if (opt.get("name") or "").strip().lower() not in drop_names
        )
        seen[key] = seen.get(key, 0) + 1
    clashes = [key for key, count in seen.items() if count > 1]
    if not clashes:
        return ""
    sample = " / ".join(clashes[0]) or "(leeg)"
    return f"{len(clashes)} combinatie(s) zouden dubbel worden, bv. {sample}"


def _plan_product(admin: MotoxAdmin, product: dict) -> dict:
    dupes = _duplicate_options(product.get("options") or [])
    base = {
        "id": product.get("id") or "",
        "handle": product.get("handle") or "",
        "title": product.get("title") or "",
        "drop": [opt.get("name") or "" for opt in dupes],
        "options": list(product.get("options") or []),
        "variants": [],
        "skip": "",
    }
    if not dupes:
        base["skip"] = "geen dubbele maat-optie"
        return base
    drop_names = {name.lower() for name in base["drop"]}
    try:
        variants = _variant_pages(admin, product["id"])
    except RuntimeError as exc:
        base["skip"] = f"varianten ophalen mislukt: {exc}"
        return base
    base["variants"] = variants
    clash = _collision(variants, drop_names)
    if clash:
        base["skip"] = clash
    return base


def _iter_catalog(admin: MotoxAdmin):
    cursor = None
    seen = 0
    while True:
        data = _gql_data(admin, _SCAN, {"cursor": cursor})
        conn = data.get("products") or {}
        nodes = conn.get("nodes") or []
        for node in nodes:
            seen += 1
            if seen % 500 == 0:
                print(f"  …{seen} producten bekeken", flush=True)
            if _duplicate_options(node.get("options") or []):
                yield node
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage") or not page.get("endCursor"):
            print(f"  …{seen} producten bekeken", flush=True)
            return
        cursor = page["endCursor"]


def _products_for_handles(admin: MotoxAdmin, handles: list[str]) -> list[dict]:
    found = []
    for raw in handles:
        handle = raw.strip().strip("/")
        if "/products/" in handle:
            handle = handle.split("/products/", 1)[1].split("?", 1)[0].strip("/")
        data = _gql_data(admin, _BY_HANDLE, {"handle": handle})
        product = data.get("productByHandle")
        if not product:
            found.append({"id": "", "handle": handle, "title": "", "options": [], "missing": True})
        else:
            found.append(product)
    return found


def _product_set_input(plan: dict) -> dict:
    drop_names = {name.lower() for name in plan["drop"]}
    kept = [
        opt
        for opt in plan["options"]
        if (opt.get("name") or "").strip().lower() not in drop_names
    ]
    product_options = []
    for opt in kept:
        values: list[dict] = []
        seen: set[str] = set()
        for variant in plan["variants"]:
            for sel in variant.get("selectedOptions") or []:
                if (sel.get("name") or "").strip() != (opt.get("name") or "").strip():
                    continue
                val = (sel.get("value") or "").strip()
                if val and val not in seen:
                    seen.add(val)
                    values.append({"name": val})
        entry: dict = {"name": opt.get("name") or "", "values": values}
        if opt.get("id"):
            entry["id"] = opt["id"]
        product_options.append(entry)

    variants = []
    for variant in plan["variants"]:
        row: dict = {
            "id": variant["id"],
            "optionValues": [
                {
                    "optionName": (sel.get("name") or "").strip(),
                    "name": (sel.get("value") or "").strip(),
                }
                for sel in (variant.get("selectedOptions") or [])
                if (sel.get("name") or "").strip().lower() not in drop_names
            ],
        }
        if variant.get("sku"):
            row["sku"] = variant["sku"]
        if variant.get("barcode"):
            row["barcode"] = variant["barcode"]
        if variant.get("price"):
            row["price"] = variant["price"]
        policy = (variant.get("inventoryPolicy") or "").strip().upper()
        if policy in ("CONTINUE", "DENY"):
            row["inventoryPolicy"] = policy
        variants.append(row)
    return {
        "id": plan["id"],
        "productOptions": product_options,
        "variants": variants,
    }


def _apply(admin: MotoxAdmin, plan: dict) -> str:
    data = _gql_data(
        admin,
        _SET,
        {"input": _product_set_input(plan), "synchronous": True},
    )
    payload = data.get("productSet") or {}
    errors = payload.get("userErrors") or []
    if errors:
        return "; ".join(str(err)[:200] for err in errors)
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "handles",
        nargs="*",
        help="Alleen deze product-handles. Leeg = de hele catalogus.",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Verwijder de optie in Motox. Zonder dit vlaggetje alleen het plan.",
    )
    args = ap.parse_args()

    admin = MotoxAdmin()
    mode = "APPLY" if args.apply else "PLAN"
    failed = 0
    seen = 0
    dropped = 0
    skipped_clash = 0
    skipped_clean = 0

    if args.handles:
        products = _products_for_handles(admin, args.handles)
    else:
        print(f"{mode}  hele catalogus", flush=True)
        products = _iter_catalog(admin)

    for product in products:
        seen += 1
        if product.get("missing"):
            print(f"{product['handle']}  niet gevonden", flush=True)
            failed += 1
            continue
        try:
            plan = _plan_product(admin, product)
        except RuntimeError as exc:
            print(f"{product.get('handle')}  fout: {exc}", flush=True)
            failed += 1
            continue
        if plan["skip"] == "geen dubbele maat-optie":
            if args.handles:
                print(f"{plan['handle']}  {plan['title']}", flush=True)
                print(f"  overgeslagen: {plan['skip']}", flush=True)
            skipped_clean += 1
            continue
        label = ", ".join(plan["drop"])
        if plan["skip"]:
            print(f"{plan['handle']}  {plan['title']}", flush=True)
            print(f"  overgeslagen ({label}): {plan['skip']}", flush=True)
            skipped_clash += 1
            continue
        if not args.apply:
            print(f"{plan['handle']}  {plan['title']}", flush=True)
            print(f"  verwijderen: {label}", flush=True)
            dropped += 1
            continue
        try:
            err = _apply(admin, plan)
        except RuntimeError as exc:
            print(f"{plan['handle']}  schrijffout: {exc}", flush=True)
            failed += 1
            continue
        if err:
            print(f"{plan['handle']}  schrijffout: {err}", flush=True)
            failed += 1
            continue
        print(f"{plan['handle']}  optie verwijderd: {label}", flush=True)
        dropped += 1

    print()
    print(
        f"{mode}  bekeken {seen}  "
        f"{'verwijderd' if args.apply else 'te verwijderen'} {dropped}  "
        f"botsing {skipped_clash}  zonder dubbel {skipped_clean}  fout {failed}",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
