"""Tests for modules.pricing_loader (geen echte input/-map nodig)."""

from __future__ import annotations

import pytest

from modules import pricing_loader


def _row_24(sku: str, price_e: str, status_k: str, gtin_x: str) -> list[str]:
    r = [""] * 24
    r[1] = sku
    r[4] = price_e
    r[10] = status_k
    r[23] = gtin_x
    return r


def test_load_price_index_reads_0150_csv(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(pricing_loader, "INPUT_DIR", str(tmp_path))
    csv_path = tmp_path / "0150_test_export.csv"
    header = ";".join([f"c{i}" for i in range(24)])
    row = ";".join(_row_24("SKU-TEST-1", "10,00", "40", "8712345678901"))
    csv_path.write_text(header + "\n" + row + "\n", encoding="utf-8")

    price_index, barcode_index, status_index = pricing_loader.load_price_index()

    assert price_index["SKU-TEST-1"] == "12.10"
    assert barcode_index["SKU-TEST-1"] == "8712345678901"
    assert status_index["SKU-TEST-1"] == "40"
    captured = capsys.readouterr()
    assert "prijzen ingelezen" in captured.out


def test_load_price_index_missing_0150_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pricing_loader, "INPUT_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError, match="0150"):
        pricing_loader.load_price_index()
