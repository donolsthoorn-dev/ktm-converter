#!/usr/bin/env python3
"""
Bouw YMM-delta tussen:
- huidige app-export (update_csv met kolom Id)
- gewenste ALL-set (ymm_APP_import_ALL_part_*.csv of ymm_APP_import_ALL.csv)

Output:
1) Add rows CSV (4 kolommen): Product Ids, Make, Model, Year
2) Delete rows CSV (1 kolom): Id

Gebruik:
  python3 scripts/build_ymm_add_delete_delta.py
  python3 scripts/build_ymm_add_delete_delta.py \
    --existing input/YMM-ktm-shop-nl.myshopify.com_1776690428-update_csv.csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.chdir(PROJECT_ROOT)

YMM_DIR = PROJECT_ROOT / "output" / "ymm"
INPUT_DIR = PROJECT_ROOT / "input"


def _norm_text(v: str) -> str:
    return re.sub(r"\s+", " ", (v or "").strip()).lower()


def _norm_pid(v: str) -> str:
    return (v or "").strip().replace("~", "")


def _norm_year(v: str) -> str:
    return re.sub(r"\s+", "", (v or "").strip())


def _key(product_ids: str, make: str, model: str, year: str) -> tuple[str, str, str, str]:
    return (_norm_pid(product_ids), _norm_text(make), _norm_text(model), _norm_year(year))


def _latest_existing_update_csv() -> str:
    files = sorted(glob.glob(str(INPUT_DIR / "YMM-*-update_csv.csv")), key=os.path.getmtime)
    return files[-1] if files else ""


def _desired_sources() -> list[str]:
    parts = sorted(glob.glob(str(YMM_DIR / "ymm_APP_import_ALL_part_*.csv")))
    if parts:
        return parts
    single = str(YMM_DIR / "ymm_APP_import_ALL.csv")
    return [single] if os.path.exists(single) else []


def _read_existing_keys_with_ids(path: str) -> tuple[dict[tuple[str, str, str, str], str], int]:
    out: dict[tuple[str, str, str, str], str] = {}
    total = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        required = {"Id", "Product Ids", "Make", "Model", "Year"}
        missing = sorted(required - set(r.fieldnames or []))
        if missing:
            raise ValueError(f"Existing CSV mist kolommen: {', '.join(missing)}")
        for row in r:
            total += 1
            k = _key(
                row.get("Product Ids") or "",
                row.get("Make") or "",
                row.get("Model") or "",
                row.get("Year") or "",
            )
            rid = (row.get("Id") or "").strip()
            # Behoud eerste id voor deze key.
            if k not in out and rid:
                out[k] = rid
    return out, total


def _read_desired_keys_with_rows(paths: list[str]) -> tuple[dict[tuple[str, str, str, str], dict], int]:
    out: dict[tuple[str, str, str, str], dict] = {}
    total = 0
    for p in paths:
        with open(p, newline="", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            required = {"Product Ids", "Make", "Model", "Year"}
            missing = sorted(required - set(r.fieldnames or []))
            if missing:
                raise ValueError(f"Desired CSV '{p}' mist kolommen: {', '.join(missing)}")
            for row in r:
                total += 1
                k = _key(
                    row.get("Product Ids") or "",
                    row.get("Make") or "",
                    row.get("Model") or "",
                    row.get("Year") or "",
                )
                if k not in out:
                    out[k] = {
                        "Product Ids": (row.get("Product Ids") or "").strip(),
                        "Make": (row.get("Make") or "").strip(),
                        "Model": (row.get("Model") or "").strip(),
                        "Year": (row.get("Year") or "").strip(),
                    }
    return out, total


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bouw YMM delta-add + delta-delete tussen huidige app-export en gewenste ALL-set."
    )
    ap.add_argument(
        "--existing",
        default=_latest_existing_update_csv(),
        help="Pad naar huidige YMM update_csv export (met Id).",
    )
    ap.add_argument(
        "--out-add",
        default=str(YMM_DIR / "ymm_delta_add_rows.csv"),
        help="Output CSV voor Add Rows (4 kolommen).",
    )
    ap.add_argument(
        "--out-delete",
        default=str(YMM_DIR / "ymm_delta_delete_ids.csv"),
        help="Output CSV voor Delete Rows (Id kolom).",
    )
    args = ap.parse_args()

    existing = (args.existing or "").strip()
    if not existing or not os.path.exists(existing):
        print("Geen bestaande YMM update_csv gevonden/meegegeven.")
        return 1

    desired_paths = _desired_sources()
    if not desired_paths:
        print("Geen gewenste YMM ALL-bron gevonden (ymm_APP_import_ALL_part_*.csv / _ALL.csv).")
        return 1

    existing_map, existing_rows = _read_existing_keys_with_ids(existing)
    desired_map, desired_rows = _read_desired_keys_with_rows(desired_paths)

    existing_keys = set(existing_map.keys())
    desired_keys = set(desired_map.keys())

    add_keys = desired_keys - existing_keys
    del_keys = existing_keys - desired_keys

    out_add = Path(args.out_add)
    out_del = Path(args.out_delete)
    out_add.parent.mkdir(parents=True, exist_ok=True)
    out_del.parent.mkdir(parents=True, exist_ok=True)

    with out_add.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Product Ids", "Make", "Model", "Year"])
        w.writeheader()
        for k in sorted(add_keys):
            w.writerow(desired_map[k])

    with out_del.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Id"])
        for k in sorted(del_keys):
            rid = existing_map.get(k, "").strip()
            if rid:
                w.writerow([rid])

    print(f"Existing bron: {existing}")
    print(f"Desired bron(nen): {len(desired_paths)} bestand(en)")
    for p in desired_paths:
        print(f"  - {p}")
    print(f"Existing rows: {existing_rows}, unieke keys: {len(existing_keys)}")
    print(f"Desired rows: {desired_rows}, unieke keys: {len(desired_keys)}")
    print(f"Delta add keys: {len(add_keys)}")
    print(f"Delta delete keys: {len(del_keys)}")
    print(f"Add CSV: {out_add}")
    print(f"Delete CSV: {out_del}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
