"""Prijs 0: alleen het hele product op draft als elke variant 0 is, en die variant niet verkopen."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _deactivate():
    return _load(
        "shopify_auto_deactivate_invalid_products",
        ROOT / "scripts" / "shopify_auto_deactivate_invalid_products.py",
    )


def _sync():
    return _load(
        "shopify_sync_from_pricelist_csv",
        ROOT / "scripts" / "shopify_sync_from_pricelist_csv.py",
    )


def _variant(sku: str, price: str) -> dict:
    return {
        "sku": sku,
        "price": price,
        "inventoryPolicy": "CONTINUE",
        "inventoryQuantity": 1,
    }


def _product(status: str) -> dict:
    return {
        "id": "gid://shopify/Product/1",
        "handle": "huq-910101xx",
        "title": "Shock spring",
        "status": status,
    }


def test_mixed_zero_price_stays_active():
    mod = _deactivate()
    row = mod._evaluate_product(
        _product("ACTIVE"),
        [_variant("91010164", "0.00"), _variant("91010165", "12.50")],
        {"91010164": "20", "91010165": "20"},
        {"91010164": 1, "91010165": 1},
    )
    assert row["reason_bad_price"] is False
    assert row["action"] == "noop"


def test_all_zero_prices_go_draft_and_are_not_reactivated():
    mod = _deactivate()
    variants = [_variant("91010164", "0.00"), _variant("91010165", "0")]
    status = {"91010164": "20", "91010165": "20"}
    stock = {"91010164": 1, "91010165": 1}
    active = mod._evaluate_product(_product("ACTIVE"), variants, status, stock)
    assert active["reason_bad_price"] is True
    assert active["action"] == "set_draft"
    draft = mod._evaluate_product(_product("DRAFT"), variants, status, stock)
    assert draft["action"] == "noop"


def test_draft_with_a_real_price_can_return_active():
    mod = _deactivate()
    row = mod._evaluate_product(
        _product("DRAFT"),
        [_variant("91010164", "0.00"), _variant("91010165", "12.50")],
        {"91010164": "20", "91010165": "20"},
        {"91010164": 1, "91010165": 1},
    )
    assert row["action"] == "set_active"


def test_allowed_zero_price_service_sku_stays_active():
    mod = _deactivate()
    row = mod._evaluate_product(
        _product("ACTIVE"),
        [_variant("NALEVERING-DEALER", "0.00")],
        {"NALEVERING-DEALER": "20"},
        {"NALEVERING-DEALER": 1},
    )
    assert row["action"] == "noop"


def test_zero_price_forces_deny_and_does_not_vote_active():
    mod = _sync()
    policy, status, published = mod.apply_zero_price_variant_rules(
        "91010164", "0.00", "CONTINUE", "ACTIVE", True
    )
    assert policy == "DENY"
    assert status is None
    assert published is None


def test_priced_variant_keeps_continue_and_active():
    mod = _sync()
    policy, status, published = mod.apply_zero_price_variant_rules(
        "91010165", "12.50", "CONTINUE", "ACTIVE", True
    )
    assert policy == "CONTINUE"
    assert status == "ACTIVE"
    assert published is True


def test_allowed_service_sku_keeps_zero_price_sale():
    mod = _sync()
    policy, status, published = mod.apply_zero_price_variant_rules(
        "wij verzenden u kosteloos een pakket", "0.00", "CONTINUE", "ACTIVE", True
    )
    assert policy == "CONTINUE"
    assert status == "ACTIVE"
    assert published is True


def test_zero_price_sku_does_not_keep_a_mixed_product_active_by_itself():
    mod = _sync()
    desired = {
        "91010164": {"product_status": None},
        "91010165": {"product_status": "ACTIVE"},
    }
    cache = {
        "91010164": [("1", "99")],
        "91010165": [("2", "99")],
    }
    assert mod.resolve_desired_product_status_by_product_id(desired, cache) == {"99": "ACTIVE"}

    only_zero = {"91010164": {"product_status": None}}
    assert mod.resolve_desired_product_status_by_product_id(only_zero, cache) == {}
