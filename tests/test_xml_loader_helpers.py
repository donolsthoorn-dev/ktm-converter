"""Regression tests for pure helpers in modules.xml_loader (no XML / network)."""

from __future__ import annotations

import pytest

from modules.xml_loader import (
    normalize_shopify_product_handle,
    slugify,
    strip_language_suffix,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Hello World", "hello-world"),
        ("  foo  bar  ", "foo-bar"),
        ("KTM 2024!", "ktm-2024"),
        ("", ""),
    ],
)
def test_slugify(raw: str, expected: str) -> None:
    assert slugify(raw) == expected


@pytest.mark.parametrize(
    ("sku", "expected"),
    [
        ("", ""),
        ("  ", ""),
        ("ABC-DE", "ABC"),
        ("SKU/NL", "SKU"),
        ("PLAIN-SKU", "PLAIN-SKU"),
        ("X-EN/FR", "X"),
    ],
)
def test_strip_language_suffix(sku: str, expected: str) -> None:
    assert strip_language_suffix(sku) == expected


@pytest.mark.parametrize(
    ("handle", "expected"),
    [
        ("", ""),
        ("  Foo-Bar_Baz  ", "foo-bar_baz"),
        ("MIXEDcase", "mixedcase"),
    ],
)
def test_normalize_shopify_product_handle(handle: str, expected: str) -> None:
    assert normalize_shopify_product_handle(handle) == expected


def test_normalize_shopify_product_handle_none_coerces_to_empty() -> None:
    assert normalize_shopify_product_handle(None) == ""  # type: ignore[arg-type]
