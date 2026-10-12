"""Zet een deploymap klaar met de filtercode en de compacte index."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOST = ROOT / "motox" / "garage" / "host"
STAGE = HOST / "stage"
BIN = ROOT / "motox" / "garage" / "data" / "fitment.bin"


def main() -> None:
    if not BIN.is_file():
        raise SystemExit("Eerst scripts/build_motox_fitment_compact.py draaien.")
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
    catalog = STAGE / "motox" / "garage" / "catalog"
    catalog.mkdir(parents=True)
    shutil.copy2(ROOT / "motox" / "garage" / "catalog" / "vehicles.json", catalog / "vehicles.json")
    ebike = STAGE / "motox" / "garage" / "extensions" / "motor-garage" / "assets"
    ebike.mkdir(parents=True)
    shutil.copy2(
        ROOT / "motox" / "garage" / "extensions" / "motor-garage" / "assets" / "ebike-models.json",
        ebike / "ebike-models.json",
    )
    data = STAGE / "data"
    data.mkdir()
    shutil.copy2(BIN, data / "fitment.bin")
    print(STAGE)


if __name__ == "__main__":
    sys.exit(main())
