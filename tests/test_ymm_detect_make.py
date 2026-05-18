"""YMM Make-kolom per actief merk."""

from __future__ import annotations

import config
from modules.ymm_export import _detect_make


def test_detect_make_hsq_brand() -> None:
    config.apply_brand("hsq")
    assert _detect_make([], []) == "Husqvarna"


def test_detect_make_wp_brand() -> None:
    config.apply_brand("wp")
    assert _detect_make([], []) == "WP"


def test_detect_make_ktm_default() -> None:
    config.apply_brand("ktm")
    assert _detect_make(["KTM 390 Adventure"], ["$M-FOO2024"]) == "KTM"


def teardown_module() -> None:
    config.apply_brand("ktm")
