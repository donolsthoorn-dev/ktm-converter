"""Tests for modules.pricing_loader (geen echte input/-map nodig)."""

from __future__ import annotations

import pytest

import config
from modules import pricing_loader


def _row_24(sku: str, price_e: str, status_k: str, gtin_x: str) -> list[str]:
    r = [""] * 24
    r[1] = sku
    r[4] = price_e
    r[10] = status_k
    r[23] = gtin_x
    return r


def _legacy_header_24() -> str:
    h = [""] * 24
    h[1] = "ArticleNumber"
    h[4] = "SalesPrice"
    h[10] = "ArticleStatus"
    h[23] = "GTIN"
    return ";".join(h)


def test_load_price_index_reads_0150_csv(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(config, "INPUT_DIR", str(tmp_path))
    csv_path = tmp_path / pricing_loader.DEFAULT_0150_CSV_NAME
    header = _legacy_header_24()
    row = ";".join(_row_24("SKU-TEST-1", "10,00", "40", "8712345678901"))
    csv_path.write_text(header + "\n" + row + "\n", encoding="utf-8")

    price_index, barcode_index, status_index = pricing_loader.load_price_index()

    assert price_index["SKU-TEST-1"] == "12.10"
    assert barcode_index["SKU-TEST-1"] == "8712345678901"
    assert status_index["SKU-TEST-1"] == "40"
    captured = capsys.readouterr()
    assert "prijzen ingelezen" in captured.out


def test_load_price_index_resolves_columns_by_name_not_position(
    tmp_path, monkeypatch, capsys
) -> None:
    """Kolommen via header (zelfde velden als 0150_35-export, andere volgorde)."""
    monkeypatch.setattr(config, "INPUT_DIR", str(tmp_path))
    monkeypatch.setenv("KTM_0150_CSV", "0150_reordered.csv")
    csv_path = tmp_path / "0150_reordered.csv"
    header = ";".join(
        [
            "ArticleNumber",
            "ignored",
            "SalesPrice",
            "ignored",
            "ignored",
            "ArticleStatus",
        ]
    )
    row = ";".join(["SKU-REORDER", "", "10,00", "", "", "40"])
    csv_path.write_text(header + "\n" + row + "\n", encoding="utf-8")

    price_index, _, status_index = pricing_loader.load_price_index()

    assert price_index["SKU-REORDER"] == "12.10"
    assert status_index["SKU-REORDER"] == "40"


def test_load_price_index_pads_truncated_rows(tmp_path, monkeypatch, capsys) -> None:
    """Regels zonder trailing lege kolommen: aanvullen t.o.v. header."""
    monkeypatch.setattr(config, "INPUT_DIR", str(tmp_path))
    csv_path = tmp_path / pricing_loader.DEFAULT_0150_CSV_NAME
    header = _legacy_header_24()
    r = [""] * 24
    r[1] = "SKU-PAD"
    r[4] = "10,00"
    r[10] = "40"
    partial = ";".join(r[:11])
    csv_path.write_text(header + "\n" + partial + "\n", encoding="utf-8")

    price_index, _, _ = pricing_loader.load_price_index()

    assert price_index["SKU-PAD"] == "12.10"


def test_load_price_index_uppercases_sku_keys(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(config, "INPUT_DIR", str(tmp_path))
    csv_path = tmp_path / pricing_loader.DEFAULT_0150_CSV_NAME
    header = _legacy_header_24()
    row = ";".join(_row_24("mixed-Case-Sku", "10,00", "40", ""))
    csv_path.write_text(header + "\n" + row + "\n", encoding="utf-8")

    price_index, _, _ = pricing_loader.load_price_index()

    assert price_index["MIXED-CASE-SKU"] == "12.10"


def test_load_price_index_missing_0150_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "INPUT_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError, match="0150"):
        pricing_loader.load_price_index()


def test_pricelist_lookup_keys_strips_wp_xml_suffix() -> None:
    assert pricing_loader.pricelist_lookup_keys("T05049-00") == ["T05049-00", "T05049"]
    assert pricing_loader.pricelist_lookup_keys("T05049") == ["T05049"]


def test_lookup_in_str_index_wp_xml_to_erp_sku() -> None:
    index = {"T05049": "25.95"}
    assert pricing_loader.lookup_in_str_index(index, "T05049-00") == "25.95"


def test_variant_pairs_for_pricelist_sku_matches_xml_suffix() -> None:
    sku_to_vp = {"T05049-00": [("111", "222")]}
    pairs = pricing_loader.variant_pairs_for_pricelist_sku(sku_to_vp, "T05049")
    assert pairs == [("111", "222")]


def test_canonical_erp_sku_from_xml_strips_when_base_in_csv() -> None:
    erp = {"T05049", "T536"}
    assert pricing_loader.canonical_erp_sku_from_xml("T05049-00", erp_sku_keys=erp) == "T05049"
    assert pricing_loader.canonical_erp_sku_from_xml("T05049", erp_sku_keys=erp) == "T05049"
    assert pricing_loader.canonical_erp_sku_from_xml("UNKNOWN-00", erp_sku_keys=erp) == "UNKNOWN-00"


def test_canonical_erp_sku_from_xml_no_keys_unchanged() -> None:
    assert pricing_loader.canonical_erp_sku_from_xml("T05049-00", erp_sku_keys=None) == "T05049-00"


def _minimal_35_z1_csv(sku: str, status: str, stock: str) -> str:
    header = ";".join(
        [
            "PricelistTable",
            "ArticleNumber",
            "ArticleName",
            "Currency",
            "SalesPrice",
            "PurchasePrice",
            "DiscountGroup",
            "MeasureUnit",
            "PriceQuantity",
            "PackageSize",
            "ArticleStatus",
            "ProductHierarchy",
            "ProductHierarchyL1",
            "ProductHierarchyL2",
            "ProductHierarchyL3",
            "ProductHierarchyL4",
            "ProductHierarchyL5",
            "ProductHierarchyL6",
            "ProductHierarchyL7",
            "PurposeOfUse",
            "PriceValidFrom",
            "StockAvailable",
            "hqETADate",
            "GTIN",
        ]
    )
    row = [""] * 24
    row[1] = sku
    row[4] = "10,00"
    row[10] = status
    row[21] = stock
    return header + "\n" + ";".join(row) + "\n"


def test_load_article_status_merges_ktm_hsq_wp(tmp_path, monkeypatch) -> None:
    """Status/stock-index laadt alle merken (niet alleen input/)."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "input").mkdir()
    (tmp_path / "input" / "hsq").mkdir()
    (tmp_path / "input" / "wp").mkdir()
    (tmp_path / "input" / "0150_35_Z1_EUR_EN_csv.csv").write_text(
        _minimal_35_z1_csv("KTM-SKU", "20", "1"), encoding="utf-8"
    )
    (tmp_path / "input" / "hsq" / "0140_35_Z1_EUR_EN_csv.csv").write_text(
        _minimal_35_z1_csv("3HS240018603", "50", "1"), encoding="utf-8"
    )
    (tmp_path / "input" / "wp" / "0910_35_Z1_EUR_EN_csv.csv").write_text(
        _minimal_35_z1_csv("T05049", "40", "2"), encoding="utf-8"
    )

    status = pricing_loader.load_article_status_from_35_z1_csv_files()
    stock = pricing_loader.load_stock_available_from_35_z1_csv_files()

    assert status["KTM-SKU"] == "20"
    assert status["3HS240018603"] == "50"
    assert status["T05049"] == "40"
    assert stock["KTM-SKU"] == 1
    assert stock["3HS240018603"] == 1
    assert stock["T05049"] == 2


def test_load_article_status_legacy_input_dir_fallback(tmp_path, monkeypatch) -> None:
    """Zonder multi-brand layout: single-dir glob blijft werken."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    only = tmp_path / "only"
    only.mkdir()
    (only / "0150_35_Z1_EUR_EN_csv.csv").write_text(
        _minimal_35_z1_csv("LEGACY-1", "30", "0"), encoding="utf-8"
    )

    status = pricing_loader.load_article_status_from_35_z1_csv_files(str(only))
    assert status["LEGACY-1"] == "30"
