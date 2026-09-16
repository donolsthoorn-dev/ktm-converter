"""Build YMM filter + fits_on metafields from VSE vehicles + brand link CSVs."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

log = logging.getLogger("ebihr.build_ymm")


def _find_vehicles_csv(raw_dir: Path) -> Path:
    candidates = list(raw_dir.rglob("VehiclesList.csv"))
    if not candidates:
        raise FileNotFoundError(f"Geen VehiclesList.csv onder {raw_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_brand_link_csvs(raw_dir: Path) -> list[Path]:
    """VSE links extract: files named like ``[ACERBIS].csv``."""
    files: list[Path] = []
    for p in raw_dir.rglob("*.csv"):
        name = p.name
        if name.startswith("[") and name.endswith("].csv"):
            files.append(p)
    return sorted(files)


def parse_vehiclelist(path: Path) -> dict[str, dict[str, str]]:
    vehicle_data: dict[str, dict[str, str]] = {}
    with path.open(encoding="utf-8", newline="") as fd:
        for row in csv.DictReader(fd):
            code = (row.get("VehicleCode") or "").strip()
            if not code:
                continue
            vehicle_data[code] = {k: (v or "") for k, v in row.items() if k != "VehicleCode"}
    log.info("VehiclesList: %d voertuigen uit %s", len(vehicle_data), path)
    return vehicle_data


def build_ymm_from_raw(raw_dir: Path) -> dict:
    """
    Returns dict with:
      ymm: year/make/model key → {ids, make, model, year}
      fits_on: partnumber → {make: {model: [years]}}
      offroad_partnumbers: set[str]
      brands_seen: list[str]
    """
    vehicles = parse_vehiclelist(_find_vehicles_csv(raw_dir))
    brand_files = _find_brand_link_csvs(raw_dir)
    if not brand_files:
        raise FileNotFoundError(
            f"Geen brand-link CSV's ([Brand].csv) onder {raw_dir}. "
            "Draai eerst ebihr_fetch (VSE links)."
        )

    ymm: dict[str, dict] = {}
    fits_on: dict[str, dict] = {}
    offroad: set[str] = set()
    brands_seen: list[str] = []

    for brand_file in brand_files:
        brand_name = brand_file.stem.strip("[]")
        brands_seen.append(brand_name)
        log.info("YMM brand file: %s", brand_file.name)
        with brand_file.open(encoding="utf-8", newline="") as fd:
            for row in csv.DictReader(fd):
                vehicle_code = (row.get("VehicleCode") or "").strip()
                partnum = (row.get("PartNumber") or "").strip()
                if not vehicle_code or not partnum:
                    continue
                info = vehicles.get(vehicle_code)
                if not info:
                    continue
                year = (info.get("VehicleYear") or "").strip()
                make = (info.get("ManufacturerName") or "").strip()
                model = (info.get("CommercialModelName") or "").strip()
                if not (year and make and model):
                    continue
                key = f"{year} {make} {model}"
                if key not in ymm:
                    ymm[key] = {"ids": [], "make": make, "model": model, "year": year}
                ymm[key]["ids"].append(partnum)

                fits_on.setdefault(partnum, {})
                fits_on[partnum].setdefault(make, {})
                fits_on[partnum][make].setdefault(model, [])
                years_list = fits_on[partnum][make][model]
                if year not in years_list:
                    years_list.append(year)
                if len(years_list) > 1:
                    fits_on[partnum][make][model] = sorted(set(years_list))

                if (info.get("UniverseName") or "").strip().upper() == "OFFROAD":
                    offroad.add(partnum)

    log.info(
        "YMM klaar: %d voertuig-keys, %d fits_on handles, %d offroad SKUs, %d brands",
        len(ymm),
        len(fits_on),
        len(offroad),
        len(brands_seen),
    )
    return {
        "ymm": ymm,
        "fits_on": fits_on,
        "offroad_partnumbers": offroad,
        "brands_seen": brands_seen,
    }


def ymm_filter_rows(ymm: dict) -> list[list[str]]:
    rows: list[list[str]] = []
    for key in sorted(ymm.keys()):
        entry = ymm[key]
        rows.append(
            [
                "~".join(entry["ids"]),
                entry["make"],
                entry["model"],
                entry["year"],
            ]
        )
    return rows


def fits_on_rows(fits_on: dict) -> list[list[str]]:
    rows: list[list[str]] = []
    for handle in sorted(fits_on.keys()):
        rows.append([handle, json.dumps(fits_on[handle], ensure_ascii=False)])
    return rows
