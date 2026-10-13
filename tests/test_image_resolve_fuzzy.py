"""Fuzzy lokale image-match: XML zonder Celum-ID ↔ schijf mét Celum-ID."""

from __future__ import annotations

from pathlib import Path

from modules.image_resolve import build_basename_index, resolve_local_image


def test_fuzzy_matches_celum_prefixed_local_file(tmp_path: Path):
    pho = tmp_path / "PHO" / "NMON"
    pho.mkdir(parents=True)
    local = pho / (
        "pho_nmon_430720_a46009901544c1a_factory_racing_front_wheel_1_6x21___sall__awsg__v1.jpg"
    )
    local.write_bytes(b"x")

    by_exact, by_lower = build_basename_index(tmp_path)
    xml_ref = (
        "PHO/NMON/pho_nmon_a46009901544c1a_factory_racing_front_wheel_1_6x21__sall__awsg__v1.jpg"
    )
    found = resolve_local_image(xml_ref, tmp_path, by_exact, by_lower)
    assert found is not None
    assert found.name == local.name


def test_exact_basename_still_wins(tmp_path: Path):
    pho = tmp_path / "PHO" / "NMON"
    pho.mkdir(parents=True)
    exact = pho / "pho_nmon_sku12345678_thing__sall__awsg__v1.jpg"
    other = pho / "pho_nmon_999_sku12345678_thing_other__sall__awsg__v1.jpg"
    exact.write_bytes(b"a")
    other.write_bytes(b"b")

    by_exact, by_lower = build_basename_index(tmp_path)
    found = resolve_local_image(exact.name, tmp_path, by_exact, by_lower)
    assert found == exact
