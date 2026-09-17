"""Tests for modules.price_markup."""

from __future__ import annotations

from decimal import Decimal

from modules.price_markup import (
    EXCLUDED_TYPES,
    MARKUP_TYPES,
    apply_price_markup_decimal,
    apply_price_markup_float,
    apply_price_markup_str,
    product_type_from_mirror_row,
    product_type_has_markup,
)


def test_excluded_types_are_not_marked_up() -> None:
    for t in EXCLUDED_TYPES:
        assert t not in MARKUP_TYPES
        assert not product_type_has_markup(t)


def test_known_types_are_marked_up() -> None:
    assert product_type_has_markup("WP - Spare Parts")
    assert product_type_has_markup("wp - spare parts")
    assert product_type_has_markup("Event Material")
    assert product_type_has_markup("HSQ - Support tool")
    assert not product_type_has_markup("WP - Jackets")
    assert not product_type_has_markup("")
    assert not product_type_has_markup(None)


def test_apply_price_markup_decimal_rounds_half_up() -> None:
    base = Decimal("10.00")
    assert apply_price_markup_decimal(base, "WP - Spare Parts") == Decimal("10.90")
    assert apply_price_markup_decimal(base, "WP - Cartridge") == Decimal("10.00")
    # 12.10 * 1.09 = 13.189 → 13.19
    assert apply_price_markup_decimal(Decimal("12.10"), "Support tool") == Decimal("13.19")
    assert apply_price_markup_decimal(None, "Support tool") is None


def test_apply_price_markup_str_and_float() -> None:
    assert apply_price_markup_str("10.00", "WP - Tools") == "10.90"
    assert apply_price_markup_str("10.00", "WP - Gloves") == "10.00"
    assert apply_price_markup_float(10.0, "Images") == 10.90
    assert apply_price_markup_float(10.0, "WP - Protection") == 10.0


def test_product_type_from_mirror_row() -> None:
    assert product_type_from_mirror_row({"type": "WP - Archiv"}) == "WP - Archiv"
    assert (
        product_type_from_mirror_row({"raw": {"productType": "Event Material"}})
        == "Event Material"
    )
    assert product_type_from_mirror_row({}) == ""
