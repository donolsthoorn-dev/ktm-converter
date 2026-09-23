"""Afgeleide ymm-velden uit een bestaande fits_on-JSON."""

from modules.ebihr.ymm_derived import derived_ymm_fields, needs_write


def test_yamaha_hot_cams_summary() -> None:
    raw = (
        '{"YAMAHA":{"WR 400 F":["1998","1999","2000"],'
        '"WR 426 F":["2001","2002"],'
        '"YZ 400 F":["1998","1999"],'
        '"YZ 426 F":["2000","2001","2002"]}}'
    )
    derived = derived_ymm_fields(raw)
    assert derived is not None
    assert derived["ymm_summary"]
    assert derived["ymm_summary"].startswith("YAMAHA")
    assert "1998-2002" in derived["ymm_summary"]
    assert "YAMAHA" in derived["fits_on_make"].split("||")
    assert "1998" in derived["fits_on_year"].split("||")
    assert "WR 400 F" in derived["fits_on_model"].split("||")


def test_empty_and_invalid() -> None:
    assert derived_ymm_fields("") is None
    assert derived_ymm_fields("{") is None
    assert derived_ymm_fields("{}") is None


def test_needs_write_when_summary_missing() -> None:
    desired = {
        "ymm_summary": "YAMAHA 400-426 1998-2002",
        "fits_on_make": "YAMAHA",
        "fits_on_model": "WR 400 F",
        "fits_on_year": "1998",
    }
    current = {"ymm_summary": "", "fits_on_make": "", "fits_on_model": "", "fits_on_year": ""}
    assert needs_write(current, desired)
    assert not needs_write(desired, desired)
