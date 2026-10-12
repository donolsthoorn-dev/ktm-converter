"""Maak de compacte KTM fitment-index voor Vercel (ktm-garage)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.motox_garage.fitment_compact import build_compact  # noqa: E402

SOURCE = ROOT / "cache" / "ktm" / "fitment_index.sqlite"
TARGET = ROOT / "ktm" / "garage" / "data" / "fitment.bin"


def main() -> None:
    if not SOURCE.is_file():
        raise SystemExit(f"Geen sqlite-index op {SOURCE}. Eerst build_ktm_fitment_index.py.")
    counts = build_compact(SOURCE, TARGET)
    size = TARGET.stat().st_size
    print(
        f"Geschreven {TARGET} ({size / 1_048_576:.1f} MB): "
        f"{counts['pairs']} koppelingen, {counts['bikes']} motoren, "
        f"{counts['fitted']} met fitment, {counts['excluded']} excluded"
    )


if __name__ == "__main__":
    main()
