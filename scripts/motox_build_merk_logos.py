#!/usr/bin/env python3
"""Bouw Motox merkenlogo's — ALLEEN uit curated, geverifieerde bronnen.

Referentie: Acerbis — breed wordmark, links uitgelijnd (~24px), puur witte canvas.

FORCE_BLACK_HANDLES (ktm, scott-bikes, troy-lee-designs, …): solid zwart op wit,
geen oranje/kleurplaten, geen holle outlines.

Geen homepage-scraping, geen Clearbit, geen favicons/og:image.
Na elke render: QA (witte randen, minimale breedte, geen zwarte canvas).
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "merk-logos"
SRC = OUT / "src-verified"
CURATED = OUT / "curated-logo-urls.json"
REPORT = OUT / "verified-build-report.json"

LEFT_PAD = 24
MAX_W, MAX_H = 452, 160
CW, CH = 500, 200
# Acerbis-achtig: content moet breed genoeg zijn (niet een smalle kolom)
MIN_CONTENT_W = 100  # Acerbis ~452; short names (S3, NGK) stay narrower
SVG_DENSITY = "400"
# Handles that must render as solid black wordmarks on white (Acerbis-style).
# No brand-color plates, no hollow outlines.
FORCE_BLACK_HANDLES = frozenset(
    {
        "ktm",
        "scott-bikes",
        "troy-lee-designs",
        "acerbis",
        "acerbis-racing",
    }
)
# Brand plate colors to punch out before forcing black (KTM orange, etc.)
FORCE_BLACK_TRANSPARENT = (
    "#f27620",  # KTM orange plate
    "#FF6600",
    "#F8001E",  # occasional red accents
)
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def magick(*args: str) -> None:
    subprocess.check_call(["magick", *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def identify_wh(path: Path) -> tuple[int, int]:
    w, h = map(
        int,
        subprocess.check_output(
            ["magick", "identify", "-format", "%w %h", str(path)], text=True
        ).split(),
    )
    return w, h


def download(sess: requests.Session, url: str, dest: Path) -> Path:
    r = sess.get(url, timeout=(6, 20), allow_redirects=True, proxies={"http": None, "https": None})
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "html" in ctype:
        raise RuntimeError(f"got HTML for {url}")
    if len(r.content) < 400:
        raise RuntimeError(f"file too small ({len(r.content)}B): {url}")
    head = r.content[:120].lstrip()
    ext = Path(urlparse(url).path).suffix.lower()
    if head.startswith(b"<svg") or b"<svg" in head or "svg" in ctype:
        ext = ".svg"
    elif ext not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}:
        if "png" in ctype:
            ext = ".png"
        elif "jpeg" in ctype or "jpg" in ctype:
            ext = ".jpg"
        else:
            ext = ".png"
    out = dest.with_suffix(ext)
    out.write_bytes(r.content)
    if out.suffix != ".svg":
        subprocess.check_call(
            ["magick", "identify", str(out)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        w, h = identify_wh(out)
        if max(w, h) < 80 or (min(w, h) < 20 and max(w, h) < 120):
            raise RuntimeError(f"source too small {w}x{h}: {url}")
    return out


def resolve_source(handle: str, spec: str | dict, sess: requests.Session | None) -> Path:
    """spec = URL string, local path, or {url|file: ...}."""
    if isinstance(spec, dict):
        if "file" in spec:
            p = Path(spec["file"])
            if not p.is_absolute():
                p = SRC / p
            if not p.exists():
                raise RuntimeError(f"local source missing: {p}")
            return p
        if "url" in spec:
            if sess is None:
                raise RuntimeError("download needs session")
            return download(sess, spec["url"], SRC / handle)
        raise RuntimeError(f"bad curated entry for {handle}")
    if spec.startswith("http://") or spec.startswith("https://"):
        if sess is None:
            raise RuntimeError("download needs session")
        return download(sess, spec, SRC / handle)
    p = Path(spec)
    if not p.is_absolute():
        p = SRC / p
    if not p.exists():
        raise RuntimeError(f"local source missing: {p}")
    return p


def render_raw(src: Path, dest: Path) -> None:
    """Rasterize source as RGBA truecolor; SVGs at high density."""
    # Force TrueColorAlpha so palette/gray sources match rgba:- byte size
    common = [
        "-colorspace",
        "sRGB",
        "-type",
        "TrueColorAlpha",
        "-alpha",
        "on",
        str(dest),
    ]
    if src.suffix.lower() == ".svg":
        magick("-background", "none", "-density", SVG_DENSITY, str(src), *common)
    else:
        magick(str(src), "-background", "none", *common)


def to_solid_black(src: Path, dest: Path) -> Path:
    """Convert logo to solid near-black ink on transparent (Acerbis-style).

    - Punch out known brand-plate colors (e.g. KTM orange)
    - Treat border-reachable near-white as background (transparent)
    - Fill enclosed white interiors (hollow outlines → solid)
    - All remaining ink → near-black, keeping soft edges via alpha
    """
    from collections import deque

    tmp = SRC / f"{src.stem}-forceblack-in.png"
    magick(
        str(src),
        "-colorspace",
        "sRGB",
        "-type",
        "TrueColorAlpha",
        "-depth",
        "8",
        str(tmp),
    )
    for color in FORCE_BLACK_TRANSPARENT:
        magick(str(tmp), "-alpha", "on", "-fuzz", "18%", "-transparent", color, str(tmp))

    w, h = identify_wh(tmp)
    raw = bytearray(subprocess.check_output(["magick", str(tmp), "-depth", "8", "rgba:-"]))
    if len(raw) != w * h * 4:
        raise RuntimeError(f"force_black rgba size mismatch for {src}")

    def lum(i: int) -> int:
        return (raw[i] + raw[i + 1] + raw[i + 2]) // 3

    bg = bytearray(w * h)
    q: deque[tuple[int, int]] = deque()
    for x in range(w):
        q.append((x, 0))
        q.append((x, h - 1))
    for y in range(h):
        q.append((0, y))
        q.append((w - 1, y))
    while q:
        x, y = q.popleft()
        if x < 0 or y < 0 or x >= w or y >= h:
            continue
        idx = y * w + x
        if bg[idx]:
            continue
        i = idx * 4
        a = raw[i + 3]
        # Transparent or near-white = background path
        if a < 40 or lum(i) > 200:
            bg[idx] = 1
            q.extend(((x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)))

    out = bytearray(w * h * 4)
    for y in range(h):
        for x in range(w):
            idx = y * w + x
            i = idx * 4
            if bg[idx]:
                out[i : i + 4] = b"\x00\x00\x00\x00"
                continue
            L = lum(i)
            a = raw[i + 3]
            if L < 210:
                strength = 255 - L
                out[i : i + 4] = bytes((18, 18, 18, min(255, max(a, strength))))
            else:
                # Enclosed white interior of outlined glyphs → solid black
                out[i : i + 4] = bytes((18, 18, 18, 255))

    rgba = SRC / f"{src.stem}-forceblack.rgba"
    rgba.write_bytes(out)
    magick("-size", f"{w}x{h}", "-depth", "8", f"rgba:{rgba}", str(dest))
    rgba.unlink(missing_ok=True)
    return dest


def prepare_for_white(src: Path, *, force_black: bool = False) -> Path:
    """Prepare logo mark for compositing onto a pure white canvas.

    - Rasterize with transparency
    - Near-black full-bleed backgrounds → transparent (logo stays)
    - Near-white ink that would vanish on white → near-black (keep colors)
    - Optional: force solid black (no color plates / hollow outlines)
    """
    raw = SRC / f"{src.stem}-prepared-raw.png"
    out = SRC / f"{src.stem}-prepared.png"
    render_raw(src, raw)
    if force_black:
        return to_solid_black(raw, out)

    # Drop opaque near-black background plates that aren't logo ink
    # (corners of source mostly black → treat black as transparent)
    try:
        w, h = identify_wh(raw)
        blob = subprocess.check_output(
            [
                "magick",
                str(raw),
                "-colorspace",
                "sRGB",
                "-type",
                "TrueColorAlpha",
                "-depth",
                "8",
                "rgba:-",
            ]
        )
    except subprocess.CalledProcessError:
        return raw
    if len(blob) != w * h * 4:
        # Re-write as explicit RGBA and retry once
        magick(
            str(raw),
            "-colorspace",
            "sRGB",
            "-type",
            "TrueColorAlpha",
            "-depth",
            "8",
            str(raw),
        )
        w, h = identify_wh(raw)
        blob = subprocess.check_output(
            ["magick", str(raw), "-depth", "8", "rgba:-"]
        )
        if len(blob) != w * h * 4:
            return raw

    def px(x: int, y: int) -> tuple[int, int, int, int]:
        i = (y * w + x) * 4
        return blob[i], blob[i + 1], blob[i + 2], blob[i + 3]

    corners = [px(0, 0), px(w - 1, 0), px(0, h - 1), px(w - 1, h - 1)]
    black_corners = sum(1 for r, g, b, a in corners if a > 200 and r + g + b < 60)
    if black_corners >= 3:
        # Full-bleed black canvas with light/colored mark → punch out black
        magick(
            str(raw),
            "-alpha",
            "on",
            "-fuzz",
            "14%",
            "-transparent",
            "black",
            "-colorspace",
            "sRGB",
            "-type",
            "TrueColorAlpha",
            "-depth",
            "8",
            str(raw),
        )
        blob = subprocess.check_output(["magick", str(raw), "-depth", "8", "rgba:-"])

    light = dark = colored = 0
    for i in range(0, len(blob), 4):
        r, g, b, a = blob[i : i + 4]
        if a < 40:
            continue
        if max(r, g, b) - min(r, g, b) > 40:
            colored += 1
        if r + g + b > 620:
            light += 1
        elif r + g + b < 220:
            dark += 1
    # White / near-white wordmarks (or light-on-black after punch-out)
    if light > dark * 2 and light > 800:
        magick(
            str(raw),
            "-colorspace",
            "sRGB",
            "-type",
            "TrueColorAlpha",
            "-channel",
            "RGB",
            "-fx",
            "u.r>0.88 && u.g>0.88 && u.b>0.88 ? 0.07 : u",
            str(out),
        )
        return out
    # After black punch-out, remaining light/mid gray ink → near-black (keep chroma)
    if black_corners >= 3 and (light + colored > 200):
        magick(
            str(raw),
            "-colorspace",
            "sRGB",
            "-type",
            "TrueColorAlpha",
            "-channel",
            "RGB",
            "-fx",
            "(u.r+u.g+u.b)/3>0.35 && abs(u.r-u.g)<0.12 && abs(u.g-u.b)<0.12 ? 0.08 : u",
            str(out),
        )
        return out
    # Light-gray logos on transparent without black plate
    if dark < 500 and light + colored > 800:
        magick(
            str(raw),
            "-colorspace",
            "sRGB",
            "-type",
            "TrueColorAlpha",
            "-channel",
            "RGB",
            "-fx",
            "(u.r+u.g+u.b)/3>0.55 && abs(u.r-u.g)<0.08 && abs(u.g-u.b)<0.08 ? 0.08 : u",
            str(out),
        )
        return out
    return raw


def fit_left(src: Path, dest: Path, *, force_black: bool = False) -> None:
    """Fit onto 500×200 white canvas, left-aligned (Acerbis-style)."""
    prepared = prepare_for_white(src, force_black=force_black)
    sized = SRC / f"{dest.stem}-sized.png"
    # Only trim transparent margins. Never trim by corner RGB — that eats
    # solid brand plates (WD-40 yellow, KTM orange, Pirelli yellow).
    w, h = identify_wh(prepared)
    blob = subprocess.check_output(["magick", str(prepared), "-depth", "8", "rgba:-"])

    def a_at(x: int, y: int) -> int:
        return blob[(y * w + x) * 4 + 3]

    transparent_corners = sum(
        1 for x, y in ((0, 0), (w - 1, 0), (0, h - 1), (w - 1, h - 1)) if a_at(x, y) < 200
    )
    ops = [
        str(prepared),
        "-colorspace",
        "sRGB",
        "-type",
        "TrueColorAlpha",
        "-background",
        "none",
    ]
    if transparent_corners >= 2:
        ops += ["-alpha", "set", "-trim", "+repage"]
    ops += ["-resize", f"{MAX_W}x{MAX_H}", str(sized)]
    magick(*ops)
    lw, lh = identify_wh(sized)
    if lw < MIN_CONTENT_W:
        raise RuntimeError(
            f"logo too narrow after fit ({lw}px < {MIN_CONTENT_W}): use a wider horizontal source"
        )
    x, y = LEFT_PAD, (CH - lh) // 2
    magick(
        "-size",
        f"{CW}x{CH}",
        "xc:white",
        str(sized),
        "-geometry",
        f"+{x}+{y}",
        "-compose",
        "over",
        "-composite",
        "-background",
        "white",
        "-alpha",
        "remove",
        "-alpha",
        "off",
        "-depth",
        "8",
        str(dest),
    )


def qa(path: Path, *, force_black: bool = False) -> None:
    """Raise if logo fails Acerbis-style quality gates."""
    raw = subprocess.check_output(["magick", str(path), "-depth", "8", "rgba:-"])
    w, h = CW, CH
    assert len(raw) == w * h * 4
    # Entire border must stay white (not only corners)
    for x in range(w):
        for y in (0, 1, h - 2, h - 1):
            i = (y * w + x) * 4
            r, g, b, a = raw[i : i + 4]
            if min(r, g, b) < 245:
                raise RuntimeError(f"non-white border at {x},{y}: {(r, g, b)}")
    for y in range(h):
        for x in (0, 1, w - 2, w - 1):
            i = (y * w + x) * 4
            r, g, b, a = raw[i : i + 4]
            if min(r, g, b) < 245:
                raise RuntimeError(f"non-white border at {x},{y}: {(r, g, b)}")

    xs, ys = [], []
    darkish = 0
    colored = 0
    for i in range(0, len(raw), 4):
        r, g, b, a = raw[i : i + 4]
        if min(r, g, b) < 245:
            xs.append((i // 4) % w)
            ys.append((i // 4) // w)
            if r + g + b < 400:
                darkish += 1
            if max(r, g, b) - min(r, g, b) > 35:
                colored += 1
    if not xs:
        raise RuntimeError("empty logo (no non-white pixels)")
    left, right, top, bottom = min(xs), max(xs), min(ys), max(ys)
    cw, ch = right - left + 1, bottom - top + 1
    if cw < MIN_CONTENT_W or ch < 24:
        raise RuntimeError(f"content too small/narrow {cw}x{ch} (want width>={MIN_CONTENT_W})")
    if left < 8 or left > 80:
        raise RuntimeError(f"bad left padding: {left}")
    # Bright yellow/red wordmarks (AFAM, Motobatt, WD-40) have few "dark" pixels
    # but plenty of saturated color — count those as valid ink too.
    if darkish < 80 and colored < 80:
        raise RuntimeError(
            f"almost no dark/colored ink (dark={darkish}, colored={colored}) — likely empty/washed out"
        )
    if force_black and colored > 80:
        raise RuntimeError(f"force_black logo still has colored pixels ({colored})")
    black = 0
    for i in range(0, len(raw), 4):
        r, g, b, a = raw[i : i + 4]
        if r + g + b < 40:
            black += 1
    if black > w * h * 0.35:
        raise RuntimeError(f"too much black fill ({black} px)")


def build_one(
    sess: requests.Session | None,
    handle: str,
    spec: str | dict,
    *,
    force: bool,
    dest_dir: Path | None = None,
) -> dict:
    dest_dir = dest_dir or OUT
    dest = dest_dir / f"merk-{handle}.png"
    force_black = handle in FORCE_BLACK_HANDLES
    if isinstance(spec, dict) and spec.get("force_black"):
        force_black = True
    if dest.exists() and not force:
        try:
            qa(dest, force_black=force_black)
            return {"handle": handle, "status": "exists_ok", "file": dest.name}
        except RuntimeError:
            pass
    SRC.mkdir(parents=True, exist_ok=True)
    try:
        src = resolve_source(handle, spec, sess)
        fit_left(src, dest, force_black=force_black)
        qa(dest, force_black=force_black)
        return {
            "handle": handle,
            "status": "ok",
            "file": dest.name,
            "src": str(src),
            "force_black": force_black,
        }
    except Exception as e:
        dest.unlink(missing_ok=True)
        return {"handle": handle, "status": "failed", "error": str(e)[:300]}


def load_curated() -> dict:
    data = json.loads(CURATED.read_text())
    # allow {"approved_handles":..., "logos": {...}} or flat {handle: url}
    if isinstance(data.get("logos"), dict):
        return data["logos"]
    return {k: v for k, v in data.items() if k not in {"note", "approved_handles", "logos"}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dest", default="", help="Output dir (default: output/merk-logos)")
    ap.add_argument(
        "--local-map",
        default="",
        help="JSON map handle→local file under src-verified (skips curated URLs)",
    )
    args = ap.parse_args()
    if args.local_map:
        curated = json.loads(Path(args.local_map).read_text())
    else:
        curated = load_curated()
    if args.only:
        only = {x.strip() for x in args.only.split(",") if x.strip()}
        curated = {k: v for k, v in curated.items() if k in only}
    dest_dir = Path(args.dest) if args.dest else OUT
    dest_dir.mkdir(parents=True, exist_ok=True)
    sess = requests.Session()
    sess.trust_env = False
    sess.headers.update({"User-Agent": UA})
    report = []
    for handle, spec in curated.items():
        print(f"{handle}: {spec}", flush=True)
        row = build_one(sess, handle, spec, force=args.force, dest_dir=dest_dir)
        print(
            f"  → {row['status']}"
            + (f" ({row.get('error')})" if row["status"] == "failed" else ""),
            flush=True,
        )
        report.append(row)
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    ok = sum(1 for r in report if r["status"] in {"ok", "exists_ok"})
    print(f"\nOK {ok}/{len(report)}  report={REPORT}")
    return 0 if ok == len(report) else 1


if __name__ == "__main__":
    raise SystemExit(main())
