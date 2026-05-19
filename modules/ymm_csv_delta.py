"""Gedeelde normalisatie en CSV-lezers voor YMM app-import (add/update/delete)."""

from __future__ import annotations

import csv
import glob
import os
import re
from pathlib import Path

YMM_ROW_KEY_FIELDS = ("Product Ids", "Make", "Model", "Year")
UPDATE_ROW_FIELDS = ("Id", *YMM_ROW_KEY_FIELDS)


def norm_text(v: str) -> str:
    return re.sub(r"\s+", " ", (v or "").strip()).lower()


def norm_product_id(v: str) -> str:
    return (v or "").strip().replace("~", "")


def norm_year(v: str) -> str:
    return re.sub(r"\s+", "", (v or "").strip())


def row_key(product_ids: str, make: str, model: str, year: str) -> tuple[str, str, str, str]:
    return (
        norm_product_id(product_ids),
        norm_text(make),
        norm_text(model),
        norm_year(year),
    )


def latest_ymm_update_csv(input_dir: Path) -> str:
    files = sorted(input_dir.glob("YMM-*-update_csv.csv"), key=os.path.getmtime)
    return str(files[-1]) if files else ""


def desired_ymm_all_paths(ymm_dir: Path) -> list[str]:
    parts = sorted(glob.glob(str(ymm_dir / "ymm_APP_import_ALL_part_*.csv")))
    if parts:
        return parts
    single = str(ymm_dir / "ymm_APP_import_ALL.csv")
    return [single] if os.path.exists(single) else []


def read_app_export_with_ids(path: str) -> tuple[dict[tuple[str, str, str, str], dict], int]:
    """
    key → {Id, Product Ids, Make, Model, Year} (eerste Id per key).
    """
    out: dict[tuple[str, str, str, str], dict] = {}
    total = 0
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        required = set(UPDATE_ROW_FIELDS)
        missing = sorted(required - set(r.fieldnames or []))
        if missing:
            raise ValueError(f"App-export mist kolommen: {', '.join(missing)}")
        for row in r:
            total += 1
            k = row_key(
                row.get("Product Ids") or "",
                row.get("Make") or "",
                row.get("Model") or "",
                row.get("Year") or "",
            )
            rid = (row.get("Id") or "").strip()
            if k in out or not rid:
                continue
            out[k] = {
                "Id": rid,
                "Product Ids": (row.get("Product Ids") or "").strip(),
                "Make": (row.get("Make") or "").strip(),
                "Model": (row.get("Model") or "").strip(),
                "Year": (row.get("Year") or "").strip(),
            }
    return out, total


def read_desired_ymm_rows(paths: list[str]) -> tuple[dict[tuple[str, str, str, str], dict], int]:
    out: dict[tuple[str, str, str, str], dict] = {}
    total = 0
    for p in paths:
        with open(p, newline="", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            missing = sorted(set(YMM_ROW_KEY_FIELDS) - set(r.fieldnames or []))
            if missing:
                raise ValueError(f"Gewenste CSV '{p}' mist kolommen: {', '.join(missing)}")
            for row in r:
                total += 1
                k = row_key(
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


def read_allowed_product_ids(path: str) -> set[str]:
    """Product Id's uit product_ids_from_xml.csv (kolom Product Id / Product SKU)."""
    allowed: set[str] = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return allowed
        pid_col = None
        for cand in ("Product Id", "product id", "Product SKU"):
            for name in r.fieldnames:
                if (name or "").strip().lower() == cand.lower():
                    pid_col = name
                    break
            if pid_col:
                break
        if not pid_col:
            pid_col = r.fieldnames[0]
        for row in r:
            pid = norm_product_id(row.get(pid_col) or "")
            if pid:
                allowed.add(pid)
    return allowed


def row_changed(app_row: dict, desired_row: dict) -> bool:
    for field in YMM_ROW_KEY_FIELDS:
        if field == "Product Ids":
            if norm_product_id(app_row.get(field, "")) != norm_product_id(
                desired_row.get(field, "")
            ):
                return True
        elif field == "Year":
            if norm_year(app_row.get(field, "")) != norm_year(desired_row.get(field, "")):
                return True
        else:
            if norm_text(app_row.get(field, "")) != norm_text(desired_row.get(field, "")):
                return True
    return False
