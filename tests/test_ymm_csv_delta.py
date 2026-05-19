"""YMM update-row key matching."""

from __future__ import annotations

from modules.ymm_csv_delta import row_changed, row_key


def test_row_key_normalizes_product_id_tilde() -> None:
    a = row_key("~123", "KTM", "390", "2024")
    b = row_key("123", "ktm", "390", "2024")
    assert a == b


def test_row_changed_detects_product_id() -> None:
    app = {
        "Id": "1",
        "Product Ids": "111",
        "Make": "KTM",
        "Model": "390",
        "Year": "2024",
    }
    des = {**app, "Product Ids": "222"}
    assert row_changed(app, des)
    assert not row_changed(app, app)
