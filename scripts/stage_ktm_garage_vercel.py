"""Zet een deploymap klaar voor ktm-garage op Vercel."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOST = ROOT / "ktm" / "garage" / "host"
STAGE = HOST / "stage"
BIN = ROOT / "ktm" / "garage" / "data" / "fitment.bin"
KTM_CATALOG = ROOT / "ktm" / "garage" / "catalog" / "vehicles.json"
EBIKE = ROOT / "motox" / "garage" / "extensions" / "motor-garage" / "assets" / "ebike-models.json"


def main() -> None:
    if not BIN.is_file():
        raise SystemExit("Eerst scripts/build_ktm_fitment_compact.py draaien.")
    if not KTM_CATALOG.is_file():
        raise SystemExit("Eerst scripts/export_ktm_vehicle_catalog.py draaien.")
    if STAGE.exists():
        shutil.rmtree(STAGE)
    STAGE.mkdir(parents=True)
    for name in ("app.py", "requirements.txt", "vercel.json", ".python-version"):
        shutil.copy2(HOST / name, STAGE / name)
    package = STAGE / "modules" / "motox_garage"
    package.mkdir(parents=True)
    (STAGE / "modules" / "__init__.py").write_text("", encoding="utf-8")
    for source in (ROOT / "modules" / "motox_garage").glob("*.py"):
        shutil.copy2(source, package / source.name)
    # server.py zoekt catalog onder motox/garage — zelfde pad, KTM-inhoud.
    catalog = STAGE / "motox" / "garage" / "catalog"
    catalog.mkdir(parents=True)
    shutil.copy2(KTM_CATALOG, catalog / "vehicles.json")
    ktm_catalog = STAGE / "ktm" / "garage" / "catalog"
    ktm_catalog.mkdir(parents=True)
    shutil.copy2(KTM_CATALOG, ktm_catalog / "vehicles.json")
    if EBIKE.is_file():
        ebike = STAGE / "motox" / "garage" / "extensions" / "motor-garage" / "assets"
        ebike.mkdir(parents=True)
        shutil.copy2(EBIKE, ebike / "ebike-models.json")
    data = STAGE / "data"
    data.mkdir()
    shutil.copy2(BIN, data / "fitment.bin")
    print(STAGE)


if __name__ == "__main__":
    sys.exit(main())
