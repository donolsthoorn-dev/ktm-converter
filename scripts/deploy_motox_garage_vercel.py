"""Bouw Motox-catalogus + fitment.bin en deploy motox-garage naar Vercel.

Bron is global.fits_on op ktm-shop-nederland (e-Bihr, motofilter). POS-kopieën
zonder fits_on leveren geen model.

Vereist: SHOPIFY_MOTOX_* in env, VERCEL_TOKEN (of ingelogde Vercel CLI).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STAGE_SCRIPT = ROOT / "scripts" / "stage_motox_garage_vercel.py"
TEAM = "team_2Y41wljbzDGs0xqwn9vGD8Ie"
PROJECT_NAME = "motox-garage"


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, cwd=str(cwd or ROOT))


def main() -> int:
    run([sys.executable, str(ROOT / "scripts" / "export_motox_vehicle_catalog.py")])
    run([sys.executable, str(ROOT / "scripts" / "build_motox_fitment_index.py")])
    run([sys.executable, str(ROOT / "scripts" / "build_motox_fitment_compact.py")])
    run([sys.executable, str(STAGE_SCRIPT)])
    stage = ROOT / "motox" / "garage" / "host" / "stage"
    token = (os.environ.get("VERCEL_TOKEN") or "").strip()
    link = [
        "npx",
        "--yes",
        "vercel@60.0.1",
        "link",
        "--yes",
        "--project",
        PROJECT_NAME,
        "--scope",
        TEAM,
    ]
    deploy = [
        "npx",
        "--yes",
        "vercel@60.0.1",
        "deploy",
        "--prod",
        "--yes",
        "--scope",
        TEAM,
    ]
    if token:
        link.extend(["--token", token])
        deploy.extend(["--token", token])
    run(link, cwd=stage)
    run(deploy, cwd=stage)
    print("Motox garage gedeployed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
