"""Cross-brand SKU → YMM union (geen volledige XML-run in unit tests)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from modules.cross_brand_ymm import (
    build_normalized_sku_ymm_lookup,
    makes_in_ymm_set,
    merge_sku_ymm_maps,
    normalize_fitment_sku,
    ymm_lookup_for_sku,
)
def test_normalize_fitment_sku() -> None:
    assert normalize_fitment_sku("a54029994500") == "A54029994500"
    assert normalize_fitment_sku("  A54029994500  ") == "A54029994500"


def test_merge_maps_union() -> None:
    ktm = {
        "A54029994500": {("KTM", "125 SX", "2020")},
    }
    hsq = {
        "A54029994500": {("Husqvarna", "FC 250", "2021")},
        "a54029994500": {("GASGAS", "TX 300", "2019")},
    }
    merged = merge_sku_ymm_maps(ktm, hsq)
    lookup = build_normalized_sku_ymm_lookup(merged)
    assert len(lookup["A54029994500"]) == 3
    got = ymm_lookup_for_sku(lookup, "a54029994500")
    assert makes_in_ymm_set(got) == {"KTM", "Husqvarna", "GASGAS"}


@pytest.mark.skipif(
    not os.path.isfile("input/CBEXPDN_KTM-DN-3141-0.xml")
    and not os.path.isdir("input"),
    reason="KTM XML niet aanwezig",
)
def test_canonical_a54029994500_integration() -> None:
    from config import resolve_brand_xml_file
    from modules.cross_brand_ymm import build_canonical_sku_to_ymm

    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    paths = []
    for bid in ("ktm", "hsq", "wp"):
        p = resolve_brand_xml_file(bid)
        if os.path.isfile(p):
            paths.append(p)
    if len(paths) < 2:
        pytest.skip("Minstens 2 merk-XML's nodig voor integratietest")

    canonical = build_canonical_sku_to_ymm(
        paths,
        filter_makes={"KTM", "Husqvarna", "GASGAS"},
    )
    sku = "A54029994500"
    found = None
    for key, tset in canonical.items():
        if normalize_fitment_sku(key) == sku:
            found = tset
            break
    assert found is not None, "SKU niet in canonical map"
    makes = makes_in_ymm_set(found)
    assert "KTM" in makes
    assert "Husqvarna" in makes
    assert "GASGAS" in makes
    assert len(found) >= 200
