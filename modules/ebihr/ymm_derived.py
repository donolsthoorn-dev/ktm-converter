"""Leid ymm_summary en de platte fits-velden af uit een fits_on-JSON."""

from __future__ import annotations

import json

from modules.metafields_manager_export import _pipe_join_sorted, _ymm_summary

SUMMARY_MAX = 255


def json_tuples(fits: dict) -> set[tuple[str, str, str]]:
    out: set[tuple[str, str, str]] = set()
    for make, models in fits.items():
        if not isinstance(models, dict):
            continue
        for model, years in models.items():
            for year in years if isinstance(years, list) else []:
                out.add((str(make), str(model), str(year)))
    return out


def derived_ymm_fields(fits_raw: str) -> dict[str, str] | None:
    """Platte velden + ymm_summary. None als er niets uit te leiden valt."""
    raw = (fits_raw or "").strip()
    if not raw:
        return None
    try:
        fits = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(fits, dict) or not fits:
        return None
    tuples = json_tuples(fits)
    if not tuples:
        return None
    return {
        "ymm_summary": (_ymm_summary(tuples) or "")[:SUMMARY_MAX],
        "fits_on_make": _pipe_join_sorted({make for make, _, _ in tuples}),
        "fits_on_model": _pipe_join_sorted({model for _, model, _ in tuples}),
        "fits_on_year": _pipe_join_sorted({year for _, _, year in tuples}),
    }


def needs_write(current: dict[str, str], desired: dict[str, str]) -> bool:
    return any((current.get(key) or "").strip() != (desired.get(key) or "").strip() for key in desired)
