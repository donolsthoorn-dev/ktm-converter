from modules.ebihr.build_products import _normalize_product, _should_include
from modules.ebihr.price_targets import (
    retail_prices_by_partnumber,
    select_price_updates,
)


def _oil_excluded_from_create() -> dict:
    """Motorolie met een voertuiglabel: hardpart, geen offroad, geen allowlist."""
    return _normalize_product(
        {
            "PartNumber": "1099489",
            "Title": "MOTUL 3000 4T motorolie - 10W40 4L",
            "SalesCategories": [{"Name": "Motorolie 4T"}],
            "VehicleUniverses": [{"Code": "04", "Name": "MAXI SCOOTER"}],
        },
        brand="MOTUL",
        group="LIQUIDS & LUBRICANTS",
        family="OIL",
        segment="OIL ENGINE 4-STROKE",
        group_code="C",
        family_code="C001",
        segment_code="C00101",
    )


def test_retail_price_is_incl_tax_not_dealer() -> None:
    prices = retail_prices_by_partnumber(
        {
            "1099489": {
                "RecommendedRetailPriceInclTaxes": 72.5032,
                "RecommendedRetailPriceExclTaxes": 59.92,
                "DealerPriceExclTaxes": 28.76,
            },
            "1099401": {"RecommendedRetailPriceInclTaxes": 79.9931},
            "0000000": {"RecommendedRetailPriceInclTaxes": 0},
            "0000001": {"DealerPriceExclTaxes": 12.5},
        }
    )
    assert prices["1099489"] == "72.50"
    assert prices["1099401"] == "79.99"
    assert "0000000" not in prices
    assert "0000001" not in prices


def test_existing_shop_sku_outside_create_filter_still_gets_price() -> None:
    product = _oil_excluded_from_create()
    assert _should_include(product, offroad_partnumbers=set()) is False

    desired = retail_prices_by_partnumber(
        {
            "1099489": {
                "RecommendedRetailPriceInclTaxes": 72.5032,
                "DealerPriceExclTaxes": 28.76,
            }
        }
    )
    by_pid, stats = select_price_updates(
        desired,
        sku_to_vid={"1099489": "v1"},
        sku_to_pid={"1099489": "p1"},
        sku_to_shop_price={"1099489": "39.72"},
        sku_to_vendor={"1099489": "MOTUL"},
    )
    assert by_pid == {"p1": [("v1", "72.50")]}
    assert stats["checked"] == 1
    assert stats["unchanged"] == 0
    assert stats["queued_variants"] == 1


def test_matching_price_is_not_written() -> None:
    by_pid, stats = select_price_updates(
        {"1099487": "18.50"},
        sku_to_vid={"1099487": "v2"},
        sku_to_pid={"1099487": "p2"},
        sku_to_shop_price={"1099487": "18.500"},
        sku_to_vendor={"1099487": "MOTUL"},
    )
    assert by_pid == {}
    assert stats["unchanged"] == 1
    assert stats["queued_variants"] == 0


def test_oem_vendor_keeps_ktm_shop_price() -> None:
    by_pid, stats = select_price_updates(
        {"7900600": "40.00"},
        sku_to_vid={"7900600": "v3"},
        sku_to_pid={"7900600": "p3"},
        sku_to_shop_price={"7900600": "10.00"},
        sku_to_vendor={"7900600": "KTM"},
    )
    assert by_pid == {}
    assert stats["skipped_oem"] == 1
    assert stats["checked"] == 0


def test_force_all_rewrites_matching_price() -> None:
    by_pid, stats = select_price_updates(
        {"1099487": "18.50"},
        sku_to_vid={"1099487": "v2"},
        sku_to_pid={"1099487": "p2"},
        sku_to_shop_price={"1099487": "18.50"},
        sku_to_vendor={"1099487": "MOTUL"},
        use_delta=False,
    )
    assert by_pid == {"p2": [("v2", "18.50")]}
    assert stats["unchanged"] == 0
