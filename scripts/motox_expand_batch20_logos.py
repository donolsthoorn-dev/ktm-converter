#!/usr/bin/env python3
"""Vul output/merk-logos/batch20 met alle actieve Motox-merken.

1. Render merken met geverifieerde lokale bronnen (local-map / src-verified)
2. GEEN wordmarks meer — ontbrekende merken blijven weg tot harvest een echt logo vindt
3. Schrijf contact-sheet(s) van alles in batch20/

Skip: none / unknown (geen echt merk).
"""
from __future__ import annotations

import json
import re
import subprocess
import unicodedata
from pathlib import Path

import importlib.util

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "merk-logos"
BATCH = OUT / "batch20"
SRC = OUT / "src-verified"
VENDORS = Path("/tmp/motox_active_vendors_exact.json")
LOCAL_MAP = BATCH / "local-map.json"
SKIP = frozenset({"none", "unknown", ""})

LEFT_PAD = 24
CW, CH = 500, 200
MAX_W, MAX_H = 452, 160
FONT = "Arial-Black"


def load_build():
    spec = importlib.util.spec_from_file_location(
        "motox_build_merk_logos", ROOT / "scripts" / "motox_build_merk_logos.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def vendor_handle(name: str) -> str:
    s = unicodedata.normalize("NFD", name.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def magick(*args: str) -> None:
    subprocess.check_call(["magick", *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def render_wordmark(label: str, dest: Path) -> None:
    """Solid black vendor name, left-aligned on white 500×200."""
    text = " ".join(label.split()).upper()
    if not text:
        raise RuntimeError("empty label")
    tmp = SRC / f"wordmark-{dest.stem}.png"
    SRC.mkdir(parents=True, exist_ok=True)

    # Long names: wrapped caption
    if len(text) > 22:
        magick(
            "-background",
            "none",
            "-fill",
            "#121212",
            "-font",
            FONT,
            "-pointsize",
            "36",
            "-size",
            "452x",
            f"caption:{text}",
            str(tmp),
        )
        magick(str(tmp), "-trim", "+repage", str(tmp))
    else:
        kern = 8 if len(text) <= 6 else (4 if len(text) <= 10 else 0)
        size = 110 if len(text) <= 4 else (96 if len(text) <= 8 else 84)
        while size >= 24:
            cmd = [
                "-background",
                "none",
                "-fill",
                "#121212",
                "-font",
                FONT,
                "-pointsize",
                str(size),
            ]
            if kern:
                cmd += ["-kerning", str(kern)]
            cmd += [f"label:{text}", str(tmp)]
            magick(*cmd)
            w, h = map(
                int,
                subprocess.check_output(
                    ["magick", "identify", "-format", "%w %h", str(tmp)], text=True
                ).split(),
            )
            if w <= MAX_W and h <= MAX_H:
                break
            size -= 4
        else:
            magick(str(tmp), "-resize", f"{MAX_W}x{MAX_H}", str(tmp))

    w, h = map(
        int,
        subprocess.check_output(
            ["magick", "identify", "-format", "%w %h", str(tmp)], text=True
        ).split(),
    )
    if h > MAX_H:
        magick(str(tmp), "-resize", f"{MAX_W}x{MAX_H}", str(tmp))
        w, h = map(
            int,
            subprocess.check_output(
                ["magick", "identify", "-format", "%w %h", str(tmp)], text=True
            ).split(),
        )
    if w < 160 and len(text) <= 8:
        scale = min(MAX_W / max(w, 1), MAX_H / max(h, 1), 200 / max(w, 1))
        if scale > 1.01:
            magick(str(tmp), "-resize", f"{int(w * scale)}x{int(h * scale)}", str(tmp))
            w, h = map(
                int,
                subprocess.check_output(
                    ["magick", "identify", "-format", "%w %h", str(tmp)], text=True
                ).split(),
            )
    x, y = LEFT_PAD, (CH - h) // 2
    magick(
        "-size",
        f"{CW}x{CH}",
        "xc:white",
        str(tmp),
        "-geometry",
        f"+{x}+{y}",
        "-compose",
        "over",
        "-composite",
        "-depth",
        "8",
        str(dest),
    )


def build_contact_sheets(batch: Path) -> list[Path]:
    files = sorted(batch.glob("merk-*.png"))
    if not files:
        return []
    # ~40 per sheet (5 cols × 8 rows) keeps sheets readable
    cols, rows = 5, 8
    per = cols * rows
    out_paths: list[Path] = []
    for i in range(0, len(files), per):
        chunk = files[i : i + per]
        n = i // per + 1
        dest = batch / (f"_contact-sheet-{n:02d}.png" if i else "_contact-sheet.png")
        # first sheet also as _contact-sheet.png; extras numbered
        if i == 0:
            dest = batch / "_contact-sheet.png"
        else:
            dest = batch / f"_contact-sheet-{n:02d}.png"
        args = [
            "montage",
            *[str(p) for p in chunk],
            "-tile",
            f"{cols}x{rows}",
            "-geometry",
            "500x200+6+6",
            "-background",
            "#eeeeee",
            "-label",
            "%t",
            str(dest),
        ]
        subprocess.check_call(["magick", *args])
        out_paths.append(dest)
    # index sheet listing count
    (batch / "_contact-sheet-index.txt").write_text(
        f"{len(files)} logos\n" + "\n".join(p.name for p in out_paths) + "\n"
    )
    return out_paths


def main() -> int:
    build = load_build()
    BATCH.mkdir(parents=True, exist_ok=True)
    SRC.mkdir(parents=True, exist_ok=True)

    local = json.loads(LOCAL_MAP.read_text()) if LOCAL_MAP.exists() else {}
    vendors = json.loads(VENDORS.read_text())
    vendors = sorted(vendors, key=lambda x: (-x["active_products"], x["vendor"]))

    # 1) re-render verified local sources
    print(f"Rendering {len(local)} verified sources…", flush=True)
    for handle, fname in local.items():
        src = SRC / fname
        if not src.exists():
            print(f"  skip missing source {handle}: {fname}", flush=True)
            continue
        dest = BATCH / f"merk-{handle}.png"
        force_black = handle in build.FORCE_BLACK_HANDLES
        try:
            build.fit_left(src, dest, force_black=force_black)
            build.qa(dest, force_black=force_black)
            print(f"  ok source {handle}", flush=True)
        except Exception as e:
            print(f"  FAIL source {handle}: {e}", flush=True)

    # Also keep already-copied approved root files that passed QA
    for p in OUT.glob("merk-*.png"):
        handle = p.stem.replace("merk-", "")
        dest = BATCH / p.name
        if dest.exists():
            continue
        try:
            force_black = handle in build.FORCE_BLACK_HANDLES
            build.qa(p, force_black=force_black)
            dest.write_bytes(p.read_bytes())
            print(f"  copied approved {handle}", flush=True)
        except Exception as e:
            print(f"  skip approved {handle}: {e}", flush=True)

    have = {p.stem.replace("merk-", "") for p in BATCH.glob("merk-*.png")}

    # 2) WORDMARKS DISABLED — use motox_harvest_merk_logos.py for real logos only
    print("WORDMARKS DISABLED (use scripts/motox_harvest_merk_logos.py)", flush=True)
    print(f"Total in batch20: {len(list(BATCH.glob('merk-*.png')))}", flush=True)

    sheets = build_contact_sheets(BATCH)
    print("Contact sheets:", ", ".join(p.name for p in sheets), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
