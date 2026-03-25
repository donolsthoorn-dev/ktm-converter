"""Smoke: kritieke modules importeren (geen zware ETL-run)."""

from __future__ import annotations


def test_import_config() -> None:
    import config

    assert config.VAT_MULTIPLIER == 1.21
    assert "Bikes" in config.DELTA_EXCLUDED_TYPES


def test_main_defines_main() -> None:
    import main

    assert callable(main.main)
