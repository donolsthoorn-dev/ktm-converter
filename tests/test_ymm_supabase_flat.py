"""Flat Metafields-kolommen afgeleid van ymm_json."""

from modules.shopify_ymm_supabase_pipeline import (
    SHOPIFY_LIST_METAFIELD_MAX_ITEMS,
    flat_columns_for_shopify_push,
    flat_columns_from_ymm_json,
    ymm_json_to_tuples,
)


def test_ymm_json_to_tuples_and_flat() -> None:
    ymm = {
        "KTM": {"125 SX": ["2020", "2021"]},
        "HUSQVARNA": {"FC 250": ["2022"]},
    }
    t = ymm_json_to_tuples(ymm)
    assert len(t) == 3
    flat = flat_columns_from_ymm_json(ymm)
    assert "KTM" in flat["fits_on_make_old"]
    assert "HUSQVARNA" in flat["fits_on_make_old"]
    assert flat["ymm_summary"]


def test_list_cap_for_shopify() -> None:
    ymm = {"KTM": {f"M{i}": ["2020"] for i in range(200)}}
    flat, warnings = flat_columns_for_shopify_push(ymm)
    assert len(flat["fits_on_model_new"]) == SHOPIFY_LIST_METAFIELD_MAX_ITEMS
    assert any("fits_on_model_new" in w for w in warnings)
