"""Compacte fitment-index voor een serverless host.

De sqlite-index is ruim 200 MB. Deze vorm bewaart dezelfde tabellen in
ongeveer 15 MB, zodat het filter op Vercel kan draaien zonder de laptop.
"""

from __future__ import annotations

import array
import sqlite3
import struct
from pathlib import Path

from modules.motox_garage.ebike import is_ebike_bike
from modules.motox_garage.fitment_index import bike_from_vehicle

MAGIC = b"MXG1"
_VERSION = 1
_HEADER = struct.Struct("<4s5I")


def build_compact(sqlite_path: Path, out_path: Path) -> dict[str, int]:
    """Schrijft ``out_path`` vanuit de sqlite-index. Geeft de tellingen terug."""
    source = sqlite3.connect(sqlite_path)
    try:
        excluded = _sorted_ids(source, "SELECT product_id FROM excluded")
        fitted = _sorted_ids(source, "SELECT product_id FROM fitted")
        bikes: dict[str, list[int]] = {}
        motorcycle: set[int] = set()
        pairs = 0
        for bike, product_id in source.execute("SELECT bike, product_id FROM fit"):
            bikes.setdefault(str(bike), []).append(int(product_id))
            pairs += 1
            if not is_ebike_bike(str(bike)):
                motorcycle.add(int(product_id))
    finally:
        source.close()
    packed = [
        (name, array.array("Q", sorted(set(ids))))
        for name, ids in sorted(bikes.items())
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("wb") as handle:
        handle.write(
            _HEADER.pack(
                MAGIC,
                _VERSION,
                len(excluded),
                len(fitted),
                len(motorcycle),
                len(packed),
            )
        )
        _write_ids(handle, excluded)
        _write_ids(handle, fitted)
        _write_ids(handle, sorted(motorcycle))
        for name, ids in packed:
            raw = name.encode("utf-8")
            if len(raw) > 65535:
                raise ValueError(f"bikenaam te lang: {name[:40]}")
            handle.write(struct.pack("<H", len(raw)))
            handle.write(raw)
            handle.write(struct.pack("<I", len(ids)))
            ids.tofile(handle)
    return {
        "excluded": len(excluded),
        "fitted": len(fitted),
        "motorcycle": len(motorcycle),
        "bikes": len(packed),
        "pairs": pairs,
    }


class CompactFitment:
    """Zelfde antwoorden als ``FitmentIndex`` voor het collectiefilter."""

    def __init__(
        self,
        excluded: set[int],
        fitted: set[int],
        motorcycle: set[int],
        bikes: dict[str, array.array],
    ) -> None:
        self.excluded = excluded
        self.fitted = fitted
        self.motorcycle = motorcycle
        self.bikes = bikes
        self._match: dict[str, set[int]] = {}

    @classmethod
    def load(cls, path: Path) -> CompactFitment | None:
        if not path.is_file():
            return None
        data = path.read_bytes()
        if len(data) < _HEADER.size or data[:4] != MAGIC:
            return None
        magic, version, n_excluded, n_fitted, n_motorcycle, n_bikes = _HEADER.unpack_from(data)
        if magic != MAGIC or version != _VERSION:
            return None
        offset = _HEADER.size
        excluded, offset = _read_ids(data, offset, n_excluded)
        fitted, offset = _read_ids(data, offset, n_fitted)
        motorcycle, offset = _read_ids(data, offset, n_motorcycle)
        bikes: dict[str, array.array] = {}
        for _ in range(n_bikes):
            if offset + 2 > len(data):
                return None
            (name_len,) = struct.unpack_from("<H", data, offset)
            offset += 2
            name = data[offset : offset + name_len].decode("utf-8")
            offset += name_len
            (n_ids,) = struct.unpack_from("<I", data, offset)
            offset += 4
            ids: array.array = array.array("Q")
            byte_len = n_ids * ids.itemsize
            ids.frombytes(data[offset : offset + byte_len])
            offset += byte_len
            bikes[name] = ids
        return cls(set(excluded), set(fitted), set(motorcycle), bikes)

    def allowed_ids(self, product_ids: list[int], vehicle: object) -> set[int]:
        if not product_ids:
            return set()
        bike = bike_from_vehicle(vehicle)
        excluded = self._members(self.excluded, product_ids)
        fitted = self._members(self.fitted, product_ids)
        motorcycle = self._members(self.motorcycle, product_ids)
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
        if not product_ids:
            return False
        return bool(self.motorcycle_ids(product_ids))

    def ebike_only_ids(self, product_ids: list[int]) -> set[int]:
        if not product_ids:
            return set()
        return self._members(self.fitted, product_ids) - self.motorcycle_ids(product_ids)

    def motorcycle_ids(self, product_ids: list[int]) -> set[int]:
        if not product_ids:
            return set()
        return self._members(self.motorcycle, product_ids)

    def _matched(self, bike: str, product_ids: list[int]) -> set[int]:
        known = self._match.get(bike)
        if known is None:
            ids = self.bikes.get(bike)
            known = set(ids) if ids is not None else set()
            self._match[bike] = known
        if not known:
            return set()
        return {product_id for product_id in product_ids if product_id in known}

    @staticmethod
    def _members(known: set[int], product_ids: list[int]) -> set[int]:
        return {product_id for product_id in product_ids if product_id in known}


def _sorted_ids(source: sqlite3.Connection, sql: str) -> list[int]:
    return sorted(int(row[0]) for row in source.execute(sql))


def _write_ids(handle, ids: list[int]) -> None:
    packed = array.array("Q", ids)
    packed.tofile(handle)


def _read_ids(data: bytes, offset: int, count: int) -> tuple[array.array, int]:
    ids: array.array = array.array("Q")
    byte_len = count * ids.itemsize
    ids.frombytes(data[offset : offset + byte_len])
    return ids, offset + byte_len
