from modules.ebihr.build_products import _barcode, _normalize_product


def _product(raw: dict) -> dict:
    return _normalize_product(
        raw,
        brand="BELL",
        group="",
        family="",
        segment="",
        group_code="",
        family_code="",
        segment_code="",
    )


def test_prefers_ean13_over_unknown_and_barcode():
    product = _product(
        {
            "PartNumber": "8006029",
            "BarCode": "970706L",
            "GtinCodes": [
                {"Code": "970706L", "Type": "UNKNOWN"},
                {"Code": "8430525191211", "Type": "EAN13"},
            ],
        }
    )
    assert _barcode(product) == "8430525191211"


def test_skips_partnumber_barcode_without_gtin():
    product = _product(
        {
            "PartNumber": "8008532004",
            "BarCode": "8008532004",
            "GtinCodes": [{"Code": "8008532004", "Type": "UNKNOWN"}],
        }
    )
    assert _barcode(product) == ""


def test_falls_back_to_valid_barcode():
    product = _product(
        {
            "PartNumber": "8005119012",
            "BarCode": "768686438868",
        }
    )
    assert _barcode(product) == "768686438868"


def test_prefers_upc_over_ean8_when_no_ean13():
    product = _product(
        {
            "PartNumber": "8004747005",
            "GtinCodes": [
                {"Code": "12345670", "Type": "EAN8"},
                {"Code": "768686175251", "Type": "UPC"},
            ],
        }
    )
    assert _barcode(product) == "768686175251"
