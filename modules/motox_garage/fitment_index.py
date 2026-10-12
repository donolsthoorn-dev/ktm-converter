"""Index: welk product past op welke motor.

Bron is ``global.fits_on`` van Motox. Een product zonder fitment blijft
zichtbaar (helm, olie, kleding). Een POS-kopie van KTM/Husqvarna/GasGas/WP
met tag ``synkro-clone-ktm-shop-nederland`` en lege ``fits_on`` hoort daar
niet bij en valt weg zodra er een motor gekozen is.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Iterator

from modules.motox_garage.ebike import is_ebike_bike, is_ebike_model
from modules.motox_garage.model import clean_token, clean_year, vehicle_id

POS_CLONE_TAG = "synkro-clone-ktm-shop-nederland"
_CHUNK = 400

_SCHEMA = """
CREATE TABLE fitted (product_id INTEGER PRIMARY KEY);
CREATE TABLE excluded (product_id INTEGER PRIMARY KEY);
CREATE TABLE fit (
  product_id INTEGER NOT NULL,
  bike TEXT NOT NULL,
  PRIMARY KEY (product_id, bike)
);
CREATE INDEX fit_bike ON fit (bike, product_id);
"""


def product_id_from_gid(value: object) -> int | None:
    text = str(value or "").strip()
    if "/" in text:
        text = text.rsplit("/", 1)[-1]
    if text.isdigit():
        return int(text)
    return None


def bike_keys(fits_on: object) -> set[str]:
    parsed = fits_on
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            return set()
    if not isinstance(parsed, dict) or not parsed:
        return set()
    keys: set[str] = set()
    for make, models in parsed.items():
        make_key = clean_token(make)
        if not make_key or not isinstance(models, dict):
            continue
        for model, years in models.items():
            model_key = clean_token(model)
            if not model_key or not isinstance(years, list):
                continue
            if is_ebike_model(make_key, model_key):
                continue
            for year in years:
                year_key = clean_year(year)
                if year_key:
                    keys.add(vehicle_id(make_key, model_key, year_key))
    return keys


def is_pos_clone(tags: object) -> bool:
    if isinstance(tags, str):
        tags = [part.strip() for part in tags.split(",")]
    if not isinstance(tags, list):
        return False
    target = POS_CLONE_TAG.casefold()
    return any(str(tag).strip().casefold() == target for tag in tags)


def bike_from_vehicle(vehicle: object) -> str:
    if not isinstance(vehicle, dict):
        return ""
    make = clean_token(vehicle.get("make"))
    model = clean_token(vehicle.get("model"))
    year = clean_year(vehicle.get("year"))
    if not make or not model or not year:
        return ""
    return vehicle_id(make, model, year)


class FitmentIndex:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()

    @classmethod
    def create(cls, path: Path) -> FitmentIndex:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        index = cls(path)
        index._conn.executescript(_SCHEMA)
        index._conn.commit()
        return index

    @classmethod
    def open(cls, path: Path) -> FitmentIndex | None:
        if not path.is_file():
            return None
        return cls(path)

    def add_product(self, product_id: int, tags: object, fits_on: object) -> str:
        """``fitted``, ``excluded`` of ``universal``."""
        keys = bike_keys(fits_on)
        if keys:
            with self._lock:
                self._conn.execute(
                    "INSERT OR IGNORE INTO fitted (product_id) VALUES (?)",
                    (product_id,),
                )
                self._conn.executemany(
                    "INSERT OR IGNORE INTO fit (product_id, bike) VALUES (?, ?)",
                    ((product_id, bike) for bike in keys),
                )
            return "fitted"
        if is_pos_clone(tags):
            with self._lock:
                self._conn.execute(
                    "INSERT OR IGNORE INTO excluded (product_id) VALUES (?)",
                    (product_id,),
                )
            return "excluded"
        return "universal"

    def finish(self) -> None:
        with self._lock:
            self._conn.commit()

    def counts(self) -> dict[str, int]:
        return {
            "fitted": self._scalar("SELECT COUNT(*) FROM fitted"),
            "excluded": self._scalar("SELECT COUNT(*) FROM excluded"),
            "pairs": self._scalar("SELECT COUNT(*) FROM fit"),
        }

    def allowed_ids(self, product_ids: list[int], vehicle: object) -> set[int]:
        if not product_ids:
            return set()
        bike = bike_from_vehicle(vehicle)
        excluded = self._members("excluded", product_ids)
        fitted = self._members("fitted", product_ids)
        motorcycle = self.motorcycle_ids(product_ids)
        matched = self._matched(bike, product_ids) if bike else set()
        allowed: set[int] = set()
        for product_id in product_ids:
            if product_id in excluded:
                continue
            if product_id in fitted and product_id not in motorcycle:
                allowed.add(product_id)
                continue
            if product_id in fitted and product_id not in matched:
                continue
            allowed.add(product_id)
        return allowed

    def allows(self, product_id: int, vehicle: object) -> bool:
        return product_id in self.allowed_ids([product_id], vehicle)

    def has_fitted(self, product_ids: list[int]) -> bool:
        """True als minstens één product motor-fitment heeft.

        Alleen e-bike-koppelingen tellen niet.
        """
        if not product_ids:
            return False
        return bool(self.motorcycle_ids(product_ids))

    def ebike_only_ids(self, product_ids: list[int]) -> set[int]:
        """Producten waarvan elke fits_on-koppeling een e-bike is."""
        if not product_ids:
            return set()
        fitted = self._members("fitted", product_ids)
        return fitted - self.motorcycle_ids(product_ids)

    def motorcycle_ids(self, product_ids: list[int]) -> set[int]:
        """Producten met minstens één motor in fits_on."""
        if not product_ids:
            return set()
        found: set[int] = set()
        for chunk in _chunks(product_ids, _CHUNK):
            marks = ",".join("?" * len(chunk))
            query = f"SELECT product_id, bike FROM fit WHERE product_id IN ({marks})"
            with self._lock:
                rows = self._conn.execute(query, chunk).fetchall()
            for product_id, bike in rows:
                if not is_ebike_bike(str(bike)):
                    found.add(int(product_id))
        return found

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _scalar(self, sql: str) -> int:
        with self._lock:
            row = self._conn.execute(sql).fetchone()
        return int(row[0] if row else 0)

    def _members(self, table: str, product_ids: list[int]) -> set[int]:
        if table not in {"excluded", "fitted"}:
            raise ValueError(table)
        found: set[int] = set()
        for chunk in _chunks(product_ids, _CHUNK):
            marks = ",".join("?" * len(chunk))
            query = f"SELECT product_id FROM {table} WHERE product_id IN ({marks})"
            with self._lock:
                rows = self._conn.execute(query, chunk).fetchall()
            found.update(int(row[0]) for row in rows)
        return found

    def _matched(self, bike: str, product_ids: list[int]) -> set[int]:
        found: set[int] = set()
        for chunk in _chunks(product_ids, _CHUNK):
            marks = ",".join("?" * len(chunk))
            query = f"SELECT product_id FROM fit WHERE bike = ? AND product_id IN ({marks})"
            with self._lock:
                rows = self._conn.execute(query, (bike, *chunk)).fetchall()
            found.update(int(row[0]) for row in rows)
        return found


def _chunks(values: list[int], size: int) -> Iterator[list[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def iter_bulk_products(path: Path) -> Iterator[tuple[int, list[str], object]]:
    """Leest een Shopify-bulk-JSONL met product-id, tags en fits_on."""
    rows: dict[int, dict] = {}
    order: list[int] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            parent = obj.get("__parentId")
            if parent:
                product_id = product_id_from_gid(parent)
                if product_id is None:
                    continue
                row = rows.setdefault(product_id, {"tags": [], "fits_on": None})
                if row["fits_on"] is None and obj.get("value"):
                    row["fits_on"] = obj.get("value")
                continue
            product_id = product_id_from_gid(obj.get("id"))
            if product_id is None:
                continue
            row = rows.setdefault(product_id, {"tags": [], "fits_on": None})
            tags = obj.get("tags") or []
            if isinstance(tags, list):
                row["tags"] = [str(tag) for tag in tags]
            meta = obj.get("metafield")
            if isinstance(meta, dict) and meta.get("value"):
                row["fits_on"] = meta.get("value")
            if product_id not in order:
                order.append(product_id)
    for product_id in order:
        row = rows[product_id]
        yield product_id, row["tags"], row["fits_on"]
