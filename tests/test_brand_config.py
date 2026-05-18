"""Tests voor merk-config en handle-prefix (geen XML/FTP)."""

from __future__ import annotations

import config
from modules.brand_config import get_brand_config, route_dir_for_filename
from modules.xml_loader import build_handle


def test_ktm_type_tag_unchanged(monkeypatch) -> None:
    config.apply_brand("ktm")
    assert config.apply_type_tag_label("Heat protection") == "Heat protection"


def test_ktm_default_paths_unchanged(monkeypatch) -> None:
    monkeypatch.delenv("BRAND", raising=False)
    config.apply_brand("ktm")
    assert config.BRAND_ID == "ktm"
    assert config.INPUT_DIR == "input"
    assert config.BASE_OUTPUT_DIR == "output"
    assert config.HANDLE_PREFIX == ""
    assert config.PRODUCTS_OUTPUT_DIR == "output/products"


def test_hsq_paths_and_prefix(monkeypatch) -> None:
    config.apply_brand("hsq")
    assert config.INPUT_DIR == "input/hsq"
    assert config.IDS_OUTPUT_DIR == "output/hsq/ids"
    assert config.YMM_OUTPUT_DIR == "output/hsq/ymm"
    assert config.METAFIELDS_OUTPUT_DIR == "output/hsq/metafields"
    assert config.HANDLE_PREFIX == "hsq-"
    assert config.get_active_brand().shopify_vendor == "HUSQVARNA"
    assert config.get_active_brand().shopify_type_tag_prefix == "HSQ - "
    assert config.apply_type_tag_label("Heat protection") == "HSQ - Heat protection"
    assert config.apply_type_tag_label("Trim parts/decals") == "HSQ - Trim parts/decals"
    assert config.apply_handle_prefix("abc123") == "hsq-abc123"
    assert config.apply_handle_prefix("hsq-abc123") == "hsq-abc123"


def test_wp_prefix(monkeypatch) -> None:
    config.apply_brand("wp")
    assert config.YMM_OUTPUT_DIR == "output/wp/ymm"
    assert config.apply_handle_prefix("sku1") == "wp-sku1"


def test_build_handle_adds_prefix_for_hsq(monkeypatch) -> None:
    config.apply_brand("hsq")
    assert build_handle("key", ["SKU123"]) == "hsq-sku123"


def test_build_handle_ktm_no_prefix(monkeypatch) -> None:
    config.apply_brand("ktm")
    assert build_handle("key", ["SKU123"]) == "sku123"


def test_ftp_routes() -> None:
    assert route_dir_for_filename("0150_35_Z1_EUR_EN_csv.csv") == "input"
    assert route_dir_for_filename("0140_35_Z1_EUR_EN_csv.csv") == "input/hsq"
    assert route_dir_for_filename("0910_35_Z1_EUR_EN_csv.csv") == "input/wp"


def test_hsq_price_csv_order() -> None:
    cfg = get_brand_config("hsq")
    assert cfg.price_csv_names[0].startswith("1100")
    assert cfg.price_csv_names[1].startswith("0140")


def teardown_module() -> None:
    config.apply_brand("ktm")
