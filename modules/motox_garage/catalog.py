"""Motorlijst (merk → model → jaren) uit ``fits_on`` / ``ymm_json``."""

from __future__ import annotations

import json
from pathlib import Path

from modules.motox_garage.ebike import is_ebike_model
from modules.motox_garage.model import clean_token, clean_year

Tree = dict[str, dict[str, set[str]]]


def catalog_path() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / "motox" / "garage" / "catalog" / "vehicles.json"


def add_fits_on(tree: Tree, fits_on: object) -> None:
    if not isinstance(fits_on, dict):
        return
    for make_raw, models in fits_on.items():
        make = clean_token(make_raw)
        if not make or not isinstance(models, dict):
            continue
        bucket = tree.setdefault(make, {})
        for model_raw, years in models.items():
            model = clean_token(model_raw)
            if not model or not isinstance(years, list):
                continue
            if is_ebike_model(make, model):
                continue
            known = bucket.setdefault(model, set())
            for year_raw in years:
                year = clean_year(year_raw)
                if year:
                    known.add(year)


def _year_sort_key(year: str) -> tuple[int, str]:
    return (int(year), year)


def catalog_from_tree(tree: Tree) -> dict:
    makes = []
    for make in sorted(tree):
        models_out = []
        for model in sorted(tree[make]):
            years = sorted(tree[make][model], key=_year_sort_key, reverse=True)
            if years:
                models_out.append({"model": model, "years": years})
        if models_out:
            makes.append({"make": make, "models": models_out})
    return {"makes": makes}


def load_catalog(path: Path | None = None) -> dict:
    target = path or catalog_path()
    if not target.is_file():
        return {"makes": []}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"makes": []}
    if not isinstance(data, dict) or not isinstance(data.get("makes"), list):
        return {"makes": []}
    return data
