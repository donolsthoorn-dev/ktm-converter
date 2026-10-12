#!/usr/bin/env python3
"""
Koppel bestaande Motox-productfoto's aan de kleur van de varianten.

De foto's staan al op het product. De bestandsnaam bevat de SKU
(bijv. 800Wx800H-1158215003-1.jpg). Per kleur wint de eerste foto waarvan
de SKU bij die kleur hoort. Die media-id gaat op elke variant van die kleur.
Zonder kleuroptie (Kleur, Kleuren, Color, Colour) gebeurt er niets.

Standaard alleen een plan. --apply schrijft via productVariantAppendMedia.
Zonder handles scant het script de hele catalogus. Alleen producten met een
kleuroptie én een SKU in de bestandsnaam worden gekoppeld.

  python3 scripts/motox_link_variant_color_images.py
  python3 scripts/motox_link_variant_color_images.py --apply
  python3 scripts/motox_link_variant_color_images.py 1158215 andere-handle
  python3 scripts/motox_link_variant_color_images.py 1158215 --apply

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

from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402

COLOR_OPTION_NAMES = frozenset({"kleur", "kleuren", "color", "colour"})
_APPEND_CHUNK = 25

_SCAN = """
query MotoxColorScan($cursor: String) {
  products(first: 50, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      handle
      options { name }
    }
  }
}
"""

_PRODUCT_QUERY = """
query MotoxColorMedia($handle: String!) {
  productByHandle(handle: $handle) {
    id
    title
    handle
    media(first: 100) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        alt
        ... on MediaImage {
          status
          image { url }
        }
      }
    }
    variants(first: 100) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        sku
        selectedOptions { name value }
        media(first: 20) { nodes { id } }
      }
    }
  }
}
"""

_MEDIA_PAGE = """
query MotoxColorMediaPage($id: ID!, $cursor: String) {
  product(id: $id) {
    media(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        alt
        ... on MediaImage {
          status
          image { url }
        }
      }
    }
  }
}
"""

_VARIANT_PAGE = """
query MotoxColorVariantPage($id: ID!, $cursor: String) {
  product(id: $id) {
    variants(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        sku
        selectedOptions { name value }
        media(first: 20) { nodes { id } }
      }
    }
  }
}
"""

_APPEND = """
mutation MotoxAppendVariantMedia(
  $productId: ID!
  $variantMedia: [ProductVariantAppendMediaInput!]!
) {
  productVariantAppendMedia(productId: $productId, variantMedia: $variantMedia) {
    productVariants { id }
    userErrors { field message code }
  }
}
"""


def _filename(url: str) -> str:
    path = (url or "").split("?", 1)[0]
    return path.rsplit("/", 1)[-1]


def _sku_in_text(text: str, skus_longest_first: list[str]) -> str:
    hay = (text or "").upper()
    if not hay:
        return ""
    for sku in skus_longest_first:
        if sku and sku in hay:
            return sku
    return ""


def _color_value(variant: dict) -> str:
    for opt in variant.get("selectedOptions") or []:
        name = (opt.get("name") or "").strip().lower()
        if name in COLOR_OPTION_NAMES:
            return (opt.get("value") or "").strip()
    return ""


def _option_labels(variant: dict) -> str:
    parts = []
    for opt in variant.get("selectedOptions") or []:
        val = (opt.get("value") or "").strip()
        if val:
            parts.append(val)
    return " / ".join(parts)


def color_media_plan(product: dict) -> dict:
    """
    Bouw het koppelplan uit een product-payload.

    Retourneert title, handle, skip (reden of ""), colors:
    [{color, filename, media_id, variants: [{sku, label, variant_id, action}]}]
    action is 'koppelen' of 'staat al'.
    """
    variants = list(product.get("variants") or [])
    media = list(product.get("media") or [])
    skus = sorted(
        {(v.get("sku") or "").strip().upper() for v in variants if (v.get("sku") or "").strip()},
        key=len,
        reverse=True,
    )
    sku_color: dict[str, str] = {}
    colored: list[dict] = []
    for variant in variants:
        color = _color_value(variant)
        sku = (variant.get("sku") or "").strip().upper()
        if sku and color and sku not in sku_color:
            sku_color[sku] = color
        if color:
            colored.append(variant)

    if not colored:
        return {
            "title": product.get("title") or "",
            "handle": product.get("handle") or "",
            "skip": "geen kleuroptie",
            "colors": [],
        }

    # Eerste bruikbare foto per kleur, in galerijvolgorde.
    chosen: dict[str, dict] = {}
    for item in media:
        status = (item.get("status") or "").upper()
        if status and status not in ("READY", "UPLOADED", "PROCESSING"):
            continue
        url = ((item.get("image") or {}) if isinstance(item.get("image"), dict) else {}).get("url") or ""
        sku = _sku_in_text(_filename(url), skus) or _sku_in_text(item.get("alt") or "", skus)
        color = sku_color.get(sku, "")
        if not color or color in chosen or not item.get("id"):
            continue
        chosen[color] = {
            "media_id": item["id"],
            "filename": _filename(url) or (item.get("alt") or "").strip() or item["id"],
        }

    order: list[str] = []
    for variant in colored:
        color = _color_value(variant)
        if color not in order:
            order.append(color)

    colors = []
    for color in order:
        pick = chosen.get(color)
        rows = []
        for variant in colored:
            if _color_value(variant) != color:
                continue
            have = {
                n.get("id")
                for n in ((variant.get("media") or {}).get("nodes") or [])
                if isinstance(n, dict) and n.get("id")
            }
            action = "staat al" if pick and pick["media_id"] in have else "koppelen"
            if not pick:
                action = "geen foto"
            rows.append(
                {
                    "sku": (variant.get("sku") or "").strip(),
                    "label": _option_labels(variant),
                    "variant_id": variant.get("id") or "",
                    "action": action,
                }
            )
        colors.append(
            {
                "color": color,
                "filename": pick["filename"] if pick else "",
                "media_id": pick["media_id"] if pick else "",
                "variants": rows,
            }
        )
    return {
        "title": product.get("title") or "",
        "handle": product.get("handle") or "",
        "skip": "",
        "colors": colors,
    }


def _gql_data(admin: MotoxAdmin, query: str, variables: dict) -> dict:
    body = admin.gql(query, variables)
    if body.get("errors"):
        raise RuntimeError(str(body.get("errors"))[:500])
    return body.get("data") or {}


def _fetch_product(admin: MotoxAdmin, handle: str) -> dict | None:
    data = _gql_data(admin, _PRODUCT_QUERY, {"handle": handle})
    product = data.get("productByHandle")
    if not product:
        return None

    media_conn = product.get("media") or {}
    media_nodes = list(media_conn.get("nodes") or [])
    page = media_conn.get("pageInfo") or {}
    while page.get("hasNextPage") and page.get("endCursor"):
        more = _gql_data(
            admin,
            _MEDIA_PAGE,
            {"id": product["id"], "cursor": page["endCursor"]},
        )
        media_conn = ((more.get("product") or {}).get("media")) or {}
        media_nodes.extend(media_conn.get("nodes") or [])
        page = media_conn.get("pageInfo") or {}

    var_conn = product.get("variants") or {}
    var_nodes = list(var_conn.get("nodes") or [])
    page = var_conn.get("pageInfo") or {}
    while page.get("hasNextPage") and page.get("endCursor"):
        more = _gql_data(
            admin,
            _VARIANT_PAGE,
            {"id": product["id"], "cursor": page["endCursor"]},
        )
        var_conn = ((more.get("product") or {}).get("variants")) or {}
        var_nodes.extend(var_conn.get("nodes") or [])
        page = var_conn.get("pageInfo") or {}

    product["media"] = media_nodes
    product["variants"] = var_nodes
    return product


def _print_plan(plan: dict) -> None:
    print(f"{plan['handle']}  {plan['title']}")
    if plan["skip"]:
        print(f"  overgeslagen: {plan['skip']}")
        return
    for group in plan["colors"]:
        if group["filename"]:
            print(f"  {group['color']}  →  {group['filename']}")
        else:
            print(f"  {group['color']}  →  geen foto met een SKU van deze kleur")
        for row in group["variants"]:
            sku = row["sku"] or "(geen sku)"
            print(f"    {sku}  {row['label']}  {row['action']}")


def _has_color_option(options: list[dict]) -> bool:
    for opt in options:
        if (opt.get("name") or "").strip().lower() in COLOR_OPTION_NAMES:
            return True
    return False


def _link_count(plan: dict) -> int:
    return sum(
        1
        for group in plan["colors"]
        for row in group["variants"]
        if row["action"] == "koppelen" and group["media_id"]
    )


def _apply_plan(admin: MotoxAdmin, product_id: str, plan: dict) -> str:
    pending: list[dict] = []
    for group in plan["colors"]:
        if not group["media_id"]:
            continue
        for row in group["variants"]:
            if row["action"] != "koppelen" or not row["variant_id"]:
                continue
            pending.append(
                {"variantId": row["variant_id"], "mediaIds": [group["media_id"]]}
            )
    if not pending:
        return ""
    errors: list[str] = []
    for i in range(0, len(pending), _APPEND_CHUNK):
        chunk = pending[i : i + _APPEND_CHUNK]
        data = _gql_data(
            admin,
            _APPEND,
            {"productId": product_id, "variantMedia": chunk},
        )
        payload = data.get("productVariantAppendMedia") or {}
        for err in payload.get("userErrors") or []:
            errors.append(str(err)[:300])
    return "; ".join(errors)


def _normalize_handle(raw: str) -> str:
    handle = raw.strip().strip("/")
    if "/products/" in handle:
        handle = handle.split("/products/", 1)[1].split("?", 1)[0].strip("/")
    return handle


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
            if _has_color_option(node.get("options") or []):
                yield node.get("handle") or ""
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage") or not page.get("endCursor"):
            print(f"  …{seen} producten bekeken", flush=True)
            return
        cursor = page["endCursor"]


def _run_handle(admin: MotoxAdmin, handle: str, *, apply: bool, brief: bool) -> str:
    """
    Verwerk één handle.

    Retourneert 'missing', 'error', 'skip', 'linked' of 'ready'.
    """
    try:
        product = _fetch_product(admin, handle)
    except RuntimeError as exc:
        print(f"{handle}  fout: {exc}", flush=True)
        return "error"
    if not product:
        print(f"{handle}  niet gevonden", flush=True)
        return "missing"
    plan = color_media_plan(product)
    pending = _link_count(plan)
    if brief and pending == 0:
        return "skip"
    if brief:
        print(f"{plan['handle']}  {plan['title']}", flush=True)
        for group in plan["colors"]:
            n = sum(1 for row in group["variants"] if row["action"] == "koppelen")
            if n and group["filename"]:
                print(f"  {group['color']}  →  {group['filename']}  ({n})", flush=True)
    else:
        print(("APPLY" if apply else "PLAN") + f"  {handle}", flush=True)
        _print_plan(plan)
    if not apply or pending == 0:
        if not brief:
            print()
        return "ready" if pending else "skip"
    err = _apply_plan(admin, product["id"], plan)
    if err:
        print(f"  schrijffout: {err}", flush=True)
        return "error"
    print(f"  geschreven: {pending} variant(en)", flush=True)
    if not brief:
        print()
    return "linked"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "handles",
        nargs="*",
        help="Product-handle(s), bv. 1158215. Leeg = de hele catalogus.",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Schrijf de koppeling naar Motox. Zonder dit vlaggetje alleen het plan.",
    )
    args = ap.parse_args()

    admin = MotoxAdmin()
    brief = not args.handles
    if brief:
        print(("APPLY" if args.apply else "PLAN") + "  hele catalogus", flush=True)
        handles = _iter_catalog(admin)
    else:
        handles = (_normalize_handle(raw) for raw in args.handles)

    counts = {"missing": 0, "error": 0, "skip": 0, "linked": 0, "ready": 0}
    for handle in handles:
        if not handle:
            continue
        outcome = _run_handle(admin, handle, apply=args.apply, brief=brief)
        counts[outcome] = counts.get(outcome, 0) + 1

    if brief:
        done = counts["linked"] + counts["ready"]
        label = "producten gekoppeld" if args.apply else "producten te koppelen"
        print(
            f"{'APPLY' if args.apply else 'PLAN'}  "
            f"{label} {done}  "
            f"overgeslagen {counts['skip']}  "
            f"fout {counts['error'] + counts['missing']}",
            flush=True,
        )
    return 1 if counts["error"] or counts["missing"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
