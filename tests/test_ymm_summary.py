"""ymm_summary: per-OEM rijke regel + join bij cross-brand."""

from modules.metafields_manager_export import _ymm_summary


def test_ktm_adventure_single_make() -> None:
    tuples = set()
    for y in range(2021, 2028):
        for m in ("790 ADVENTURE", "890 ADVENTURE R"):
            tuples.add(("KTM", m, str(y)))
    assert _ymm_summary(tuples) == "KTM 790-890 (STREET) 2021-2027"


def test_cross_brand_joined() -> None:
    tuples = set()
    for y in range(2021, 2028):
        tuples.add(("KTM", "790 ADVENTURE", str(y)))
        tuples.add(("KTM", "890 ADVENTURE R", str(y)))
    for y in range(2022, 2027):
        tuples.add(("Husqvarna", "NORDEN 901", str(y)))
    s = _ymm_summary(tuples)
    assert "KTM 790-890 (STREET) 2021-2027" in s
    assert " | " in s
    assert "HUSQVARNA" in s.upper()
