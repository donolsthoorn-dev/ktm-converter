"""Bouw catalogus + fitment.bin en deploy ktm-garage naar Vercel.

Vereist: SUPABASE_* in env, VERCEL_TOKEN (of ingelogde Vercel CLI).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STAGE_SCRIPT = ROOT / "scripts" / "stage_ktm_garage_vercel.py"
PROJECT_JSON = ROOT / "ktm" / "garage" / "host" / "stage" / ".vercel" / "project.json"
TEAM = "team_2Y41wljbzDGs0xqwn9vGD8Ie"
PROJECT_ID = "prj_BQgxirE0yhhk8k4DFgWgxago3bDL"


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, cwd=str(cwd or ROOT))


def main() -> int:
    run([sys.executable, str(ROOT / "scripts" / "export_ktm_vehicle_catalog.py")])
    run([sys.executable, str(ROOT / "scripts" / "build_ktm_fitment_index.py")])
    run([sys.executable, str(ROOT / "scripts" / "build_ktm_fitment_compact.py")])
    # Sync catalog into theme extension assets for the garage picker.
    dest = ROOT / "ktm" / "garage" / "extensions" / "motor-garage" / "assets" / "vehicles.json"
    src = ROOT / "ktm" / "garage" / "catalog" / "vehicles.json"
    if src.is_file() and dest.parent.is_dir():
        dest.write_bytes(src.read_bytes())
        print(f"catalog → {dest}", flush=True)
    run([sys.executable, str(STAGE_SCRIPT)])
    stage = ROOT / "ktm" / "garage" / "host" / "stage"
    stage.joinpath(".vercel").mkdir(parents=True, exist_ok=True)
    PROJECT_JSON.write_text(
        json.dumps(
            {
                "projectId": PROJECT_ID,
                "orgId": TEAM,
                "projectName": "ktm-garage",
            }
        ),
        encoding="utf-8",
    )
    token = (os.environ.get("VERCEL_TOKEN") or "").strip()
    cmd = [
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
        cmd.extend(["--token", token])
    run(cmd, cwd=stage)
    print("KTM garage gedeployed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
