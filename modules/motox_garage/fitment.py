"""Of een product past op de actieve motor, op basis van ``fits_on`` JSON."""

from __future__ import annotations

from modules.motox_garage.model import clean_token, clean_year

# none: geen motor gekozen. universal: product heeft geen motor-fitment.
# yes / no: fitment aanwezig en de motor zit er wel of niet in.
STATUS_NONE = "none"
STATUS_UNIVERSAL = "universal"
STATUS_YES = "yes"
STATUS_NO = "no"


def _lookup(mapping: dict, key: str) -> object | None:
    if key in mapping:
        return mapping[key]
    folded = key.casefold()
    for candidate, value in mapping.items():
        if str(candidate).casefold() == folded:
            return value
    return None


def fit_status(fits_on: object, vehicle: object) -> str:
    if not isinstance(vehicle, dict):
        return STATUS_NONE
    make = clean_token(vehicle.get("make"))
    model = clean_token(vehicle.get("model"))
    year = clean_year(vehicle.get("year"))
    if not make or not model or not year:
        return STATUS_NONE
    if not isinstance(fits_on, dict) or not fits_on:
        return STATUS_UNIVERSAL
    models = _lookup(fits_on, make)
    if not isinstance(models, dict):
        return STATUS_NO
    years = _lookup(models, model)
    if not isinstance(years, list):
        return STATUS_NO
    known = {clean_year(item) for item in years}
    known.discard("")
    if year in known:
        return STATUS_YES
    return STATUS_NO
