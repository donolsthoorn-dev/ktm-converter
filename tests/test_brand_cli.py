"""brand_cli: vroege --brand parse."""

from __future__ import annotations

import os
import sys

import config
from modules.brand_cli import bootstrap_brand_from_argv


def test_bootstrap_sets_env(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["export_product_ids_and_ymm.py", "--brand", "wp", "--help"],
    )
    monkeypatch.delenv("BRAND", raising=False)
    brand = bootstrap_brand_from_argv()
    assert brand == "wp"
    assert os.environ["BRAND"] == "wp"
    config.apply_brand("wp")
    assert config.BRAND_ID == "wp"
    assert config.YMM_OUTPUT_DIR == "output/wp/ymm"


def teardown_module() -> None:
    import os

    os.environ.pop("BRAND", None)
    config.apply_brand("ktm")
