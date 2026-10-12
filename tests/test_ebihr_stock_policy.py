from modules.ebihr.build_products import inventory_policy_from_bihr_stock


def test_bihr_stock_value_positive_is_continue() -> None:
    assert inventory_policy_from_bihr_stock({"Value": 4}) == "CONTINUE"
    assert inventory_policy_from_bihr_stock({"value": "1"}) == "CONTINUE"


def test_bihr_stock_value_zero_is_deny() -> None:
    assert inventory_policy_from_bihr_stock({"Value": 0}) == "DENY"
    assert inventory_policy_from_bihr_stock({"Value": "0"}) == "DENY"


def test_missing_bihr_stock_row_skips_update() -> None:
    assert inventory_policy_from_bihr_stock(None) is None
