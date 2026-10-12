"""E-bike-modellen uit de Bihr VSE (UniverseName E-BIKE).

Die horen niet in de motorkiezer, het motorfilter of de past-op-tekst.
Geen enkel model komt ook als motor voor.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from modules.motox_garage.model import clean_token

_PATH = (
    Path(__file__).resolve().parents[2]
    / "motox"
    / "garage"
    / "extensions"
    / "motor-garage"
    / "assets"
    / "ebike-models.json"
)


@lru_cache(maxsize=1)
def ebike_models() -> frozenset[tuple[str, str]]:
    raw = json.loads(_PATH.read_text(encoding="utf-8"))
    pairs: set[tuple[str, str]] = set()
    for item in raw:
        if not isinstance(item, list) or len(item) != 2:
            continue
        make = clean_token(item[0])
        model = clean_token(item[1])
        if make and model:
            pairs.add((make, model))
    return frozenset(pairs)


def is_ebike_model(make: object, model: object) -> bool:
    return (clean_token(make), clean_token(model)) in ebike_models()


def is_ebike_bike(bike: str) -> bool:
    parts = str(bike or "").split("::")
    if len(parts) < 2:
        return False
    return is_ebike_model(parts[0], parts[1])
