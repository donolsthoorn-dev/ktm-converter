"""Gedeelde regels: wanneer een Shopify-product niet op de Online Store hoort."""

from __future__ import annotations

from modules.pricing_loader import lookup_stock_code_in_index, normalize_sku_key, pricelist_lookup_keys


def variant_is_sold_out(v: dict) -> bool:
    policy = (
        v.get("inventoryPolicy") or v.get("inventory_policy") or ""
    ).strip().upper()
    if policy != "DENY":
        return False
    qty = v.get("inventoryQuantity")
    if qty is None:
        qty = v.get("inventory_quantity")
    if qty is None:
        return False
    try:
        return int(qty) <= 0
    except (TypeError, ValueError):
        return False


def product_all_variants_sold_out(variants: list[dict]) -> bool:
    return bool(variants) and all(variant_is_sold_out(v) for v in variants)


def variant_qty_no_stock(v: dict) -> bool:
    qty = v.get("inventoryQuantity")
    if qty is None:
        qty = v.get("inventory_quantity")
    if qty is None:
        return False
    try:
        return int(qty) <= 0
    except (TypeError, ValueError):
        return False


def product_all_variants_shopify_no_stock(variants: list[dict]) -> bool:
    """True als elke variant in Shopify quantity <= 0 heeft (onafhankelijk van inventory policy)."""
    return bool(variants) and all(variant_qty_no_stock(v) for v in variants)


def product_all_variants_erp_stock_zero(
    variants: list[dict],
    stock_by_sku: dict[str, int],
) -> bool:
    if not variants or not stock_by_sku:
        return False
    for v in variants:
        sku = normalize_sku_key(v.get("sku"))
        if not sku:
            return False
        code = lookup_stock_code_in_index(stock_by_sku, sku)
        if code != 0:
            return False
    return True


def product_unavailable_for_webshop(
    variants: list[dict],
    *,
    stock_by_sku: dict[str, int] | None = None,
) -> bool:
    """
    Niet beschikbaar voor de webshop:
    - uitverkocht in Shopify (DENY + qty <= 0 op alle varianten), of
    - ERP StockAvailable=0 op alle varianten én ook geen voorraad in Shopify.
    """
    if product_all_variants_sold_out(variants):
        return True
    if (
        stock_by_sku
        and product_all_variants_erp_stock_zero(variants, stock_by_sku)
        and product_all_variants_shopify_no_stock(variants)
    ):
        return True
    return False


def unavailable_reason_label(
    variants: list[dict],
    *,
    stock_by_sku: dict[str, int] | None = None,
) -> str | None:
    if product_all_variants_sold_out(variants):
        return "uitverkocht"
    if (
        stock_by_sku
        and product_all_variants_erp_stock_zero(variants, stock_by_sku)
        and product_all_variants_shopify_no_stock(variants)
    ):
        return "erp_stock_0"
    return None
