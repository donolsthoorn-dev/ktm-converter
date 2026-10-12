"""Garage-model: normaliseren en samenvoegen van cookie en klantaccount.

Een motor is merk + model + jaar, opgeslagen zoals in ``fits_on`` (hoofdletters).
``activeId`` wijst naar de motor die de header en later het filter aanstuurt.
"""

from __future__ import annotations

import json
from typing import Any

MAX_VEHICLES = 20
YEAR_MIN = 1950
YEAR_MAX = 2040


def clean_token(value: object) -> str:
    text = " ".join(str(value if value is not None else "").strip().upper().split())
    if not text or len(text) > 80:
        return ""
    return text


def clean_year(value: object) -> str:
    text = str(value if value is not None else "").strip()
    if len(text) == 4 and text.isdigit():
        year = int(text)
        if YEAR_MIN <= year <= YEAR_MAX:
            return text
    return ""


def vehicle_id(make: str, model: str, year: str) -> str:
    return f"{make}::{model}::{year}"


def normalize_vehicle(raw: object) -> dict[str, str] | None:
    if not isinstance(raw, dict):
        return None
    make = clean_token(raw.get("make"))
    model = clean_token(raw.get("model"))
    year = clean_year(raw.get("year"))
    if not make or not model or not year:
        return None
    return {
        "id": vehicle_id(make, model, year),
        "make": make,
        "model": model,
        "year": year,
    }


def empty_garage() -> dict[str, Any]:
    return {"activeId": None, "vehicles": []}


def normalize_garage(raw: object) -> dict[str, Any]:
    """Valideert een garage. Verzonnen geen actieve motor als die ontbreekt."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return empty_garage()
    if not isinstance(raw, dict):
        return empty_garage()
    vehicles: list[dict[str, str]] = []
    seen: set[str] = set()
    items = raw.get("vehicles")
    if isinstance(items, list):
        for item in items:
            vehicle = normalize_vehicle(item)
            if vehicle is None or vehicle["id"] in seen:
                continue
            seen.add(vehicle["id"])
            vehicles.append(vehicle)
            if len(vehicles) >= MAX_VEHICLES:
                break
    active = raw.get("activeId")
    if not isinstance(active, str) or active not in seen:
        active = None
    return {"activeId": active, "vehicles": vehicles}


def coerce_active(garage: dict[str, Any]) -> dict[str, Any]:
    """Zet een actieve motor als er voertuigen zijn en er nog geen gekozen is."""
    normalized = normalize_garage(garage)
    if normalized["activeId"] is None and normalized["vehicles"]:
        normalized["activeId"] = normalized["vehicles"][-1]["id"]
    return normalized


def merge_garages(account: object, cookie: object) -> dict[str, Any]:
    """Voegt cookie-motoren toe aan het account.

    Volgorde: eerst het account, daarna motoren die alleen in het cookie staan.
    De actieve motor van het account wint. Ontbreekt die, dan die van het cookie.
    """
    base = normalize_garage(account)
    extra = normalize_garage(cookie)
    by_id = {vehicle["id"]: vehicle for vehicle in base["vehicles"]}
    order = [vehicle["id"] for vehicle in base["vehicles"]]
    for vehicle in extra["vehicles"]:
        if vehicle["id"] in by_id:
            continue
        if len(order) >= MAX_VEHICLES:
            break
        by_id[vehicle["id"]] = vehicle
        order.append(vehicle["id"])
    vehicles = [by_id[vid] for vid in order]
    ids = set(order)
    active = None
    if base["activeId"] in ids:
        active = base["activeId"]
    elif extra["activeId"] in ids:
        active = extra["activeId"]
    elif vehicles:
        active = vehicles[-1]["id"]
    return {"activeId": active, "vehicles": vehicles}
