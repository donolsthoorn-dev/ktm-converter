"""YMM Make-kolom per actief merk."""

from __future__ import annotations

import config
from modules.ymm_export import (
    DEFAULT_YMM_OEM_MAKES,
    _detect_make,
    _make_from_model_title,
    resolve_ymm_make_filter,
)


def test_detect_make_hsq_brand() -> None:
    config.apply_brand("hsq")
    assert _detect_make([], []) == "Husqvarna"


def test_detect_make_wp_kawasaki_from_model_title() -> None:
    config.apply_brand("wp")
    assert _detect_make(["Kawasaki ZX-6R 2022"], []) == "Kawasaki"


def test_detect_make_wp_ktm_duke() -> None:
    config.apply_brand("wp")
    assert _detect_make(["250 Duke 2026"], []) == "KTM"


def test_detect_make_wp_husqvarna_tc() -> None:
    config.apply_brand("wp")
    assert _detect_make(["TC 250 2023"], []) == "Husqvarna"


def test_detect_make_wp_not_vendor_wp() -> None:
    config.apply_brand("wp")
    assert _detect_make(["Yamaha YZ 250F 2020"], []) == "Yamaha"
    assert _make_from_model_title("1290 Super Adventure T 2024") == "KTM"


def test_detect_make_ktm_default() -> None:
    config.apply_brand("ktm")
    assert _detect_make(["KTM 390 Adventure"], ["$M-FOO2024"]) == "KTM"


def test_detect_make_hsq_ktm_from_model_title() -> None:
    config.apply_brand("hsq")
    assert _detect_make(["250 Duke 2026"], []) == "KTM"


def test_resolve_ymm_make_filter_default_oem_group() -> None:
    assert resolve_ymm_make_filter() == set(DEFAULT_YMM_OEM_MAKES)


def test_resolve_ymm_make_filter_aliases() -> None:
    assert resolve_ymm_make_filter(["ktm", "HUSQVARNA", "gasgas"]) == set(
        DEFAULT_YMM_OEM_MAKES
    )


def test_resolve_ymm_make_filter_all_makes() -> None:
    assert resolve_ymm_make_filter(all_makes=True) is None


def teardown_module() -> None:
    config.apply_brand("ktm")
