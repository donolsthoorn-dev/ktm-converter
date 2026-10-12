#!/usr/bin/env python3
"""Haal MyBihr-merkenlogo's 1-op-1 binnen — geen ImageMagick-render.

Bron: https://www.mybihr.com/ww/en/all-brands
Media: https://api.mybihr.com/medias/BRN-*.png?...

- Download alle Bihr-logo's naar output/merk-logos/bihr-raw/{slug}.png
- Kopieer Motox-matches naar batch20/merk-{handle}.png (alleen JPEG→PNG
  formaatconversie indien nodig; geen canvas/kleur-processing)
- Optioneel: --upload naar Motox Shopify Files
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "merk-logos"
BATCH = OUT / "batch20"
BIHR_JSON = BATCH / "bihr-brands.json"
RAW = OUT / "bihr-raw"
REPORT = BATCH / "bihr-1to1-report.json"
VENDORS = Path("/tmp/motox_active_vendors_exact.json")

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

ALIASES: dict[str, str] = {
    "haan": "haan-wheels",
    "koubalink": "koubalinks",
    "fly": "fly-racing",
    "d-i-d": "d-i-d",
    "did": "d-i-d",
    "jt": "jt-sprockets",
    "jt-sprocket": "jt-sprockets",
    "twinair": "twin-air",
    "twin-air-filter": "twin-air",
    "brembo-racing": "brembo",
    "scott-bikes": "scott",
    "r-g": "r-g-racing",
    "risk": "risk-racing",
}


def vendor_handle(name: str) -> str:
    s = unicodedata.normalize("NFD", name.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def bihr_index(items: list[dict]) -> dict[str, dict]:
    idx: dict[str, dict] = {}
    for it in items:
        slug = (it.get("slug") or "").strip()
        if slug:
            idx[slug] = it
        idx[vendor_handle(it["name"])] = it
    return idx


def resolve_bihr(handle: str, idx: dict[str, dict]) -> dict | None:
    if handle in idx:
        return idx[handle]
    if handle in ALIASES and ALIASES[handle] in idx:
        return idx[ALIASES[handle]]
    for alt in (
        handle.replace("-racing", ""),
        handle.replace("-bikes", ""),
        handle.replace("-bike", ""),
        handle.replace("-filter", ""),
        handle + "-racing",
        handle + "-wheels",
    ):
        if alt in idx:
            return idx[alt]
        if alt in ALIASES and ALIASES[alt] in idx:
            return idx[ALIASES[alt]]
    return None


def is_jpeg(data: bytes) -> bool:
    return data[:3] == b"\xff\xd8\xff"


def is_png(data: bytes) -> bool:
    return data[:8] == b"\x89PNG\r\n\x1a\n"


def download(sess: requests.Session, url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = sess.get(
        url,
        timeout=(10, 30),
        allow_redirects=True,
        proxies={"http": None, "https": None},
    )
    r.raise_for_status()
    body = r.content
    if len(body) < 200:
        raise RuntimeError(f"too small ({len(body)}B)")
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "html" in ctype or body.lstrip()[:5].lower() in (b"<!doc", b"<html", b"<svg"):
        # Bihr missing-product placeholder
        raise RuntimeError(f"not an image ({ctype or 'unknown'})")

    # Always store as .png name for Motox CDN convention.
    # JPEG bytes → lossless re-wrap to real PNG (no colour/canvas processing).
    out = dest.with_suffix(".png")
    if is_png(body):
        out.write_bytes(body)
        return out
    if is_jpeg(body) or "jpeg" in ctype or "jpg" in ctype:
        tmp = dest.with_suffix(".jpg")
        tmp.write_bytes(body)
        subprocess.check_call(
            ["magick", str(tmp), "-strip", f"PNG32:{out}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        tmp.unlink(missing_ok=True)
        return out
    # Unknown: write as-is and hope identify accepts it
    out.write_bytes(body)
    subprocess.check_call(
        ["magick", "identify", str(out)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return out


def load_motox_env() -> None:
    env_path = ROOT / ".env.motox"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def build_contact_sheets(batch: Path) -> list[Path]:
    files = sorted(batch.glob("merk-*.png"))
    if not files:
        return []
    cols, rows = 5, 8
    per = cols * rows
    out_paths: list[Path] = []
    for i in range(0, len(files), per):
        chunk = files[i : i + per]
        n = i // per + 1
        dest = batch / ("_contact-sheet.png" if i == 0 else f"_contact-sheet-{n:02d}.png")
        subprocess.check_call(
            [
                "magick",
                "montage",
                *[str(p) for p in chunk],
                "-tile",
                f"{cols}x{rows}",
                "-geometry",
                "256x128+8+8",
                "-background",
                "#f5f5f5",
                "-label",
                "%t",
                str(dest),
            ]
        )
        out_paths.append(dest)
    (batch / "_contact-sheet-index.txt").write_text(
        f"{len(files)} logos (1:1 Bihr)\n" + "\n".join(p.name for p in out_paths) + "\n"
    )
    return out_paths


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--upload",
        action="store_true",
        help="Upload merk-*.png naar Motox Shopify Files via image_manager",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Alleen eerste N Motox-matches (debug)",
    )
    args = ap.parse_args()

    if not BIHR_JSON.exists():
        raise SystemExit(f"missing {BIHR_JSON}")
    if not VENDORS.exists():
        raise SystemExit(f"missing {VENDORS}")

    items = json.loads(BIHR_JSON.read_text())
    idx = bihr_index(items)
    vendors = json.load(VENDORS.open())

    BATCH.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)

    for old in BATCH.glob("merk-*.png"):
        old.unlink()

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept": "image/*,*/*"})

    # 1) Download ALL Bihr brands once
    raw_ok: list[str] = []
    raw_fail: list[dict] = []
    print(f"Downloading {len(items)} Bihr logos → {RAW}")
    for it in items:
        slug = it.get("slug") or vendor_handle(it["name"])
        url = it.get("logoUrl") or ""
        if not url:
            raw_fail.append({"slug": slug, "error": "no logoUrl"})
            continue
        try:
            download(sess, url, RAW / slug)
            raw_ok.append(slug)
        except Exception as e:
            raw_fail.append({"slug": slug, "error": str(e)})
            print(f"  FAIL raw {slug}: {e}")

    print(f"Bihr raw: {len(raw_ok)} ok, {len(raw_fail)} fail")

    # 2) Map Motox vendors → copy 1:1
    matched: list[dict] = []
    missing: list[dict] = []
    copied: list[str] = []
    copy_fail: list[dict] = []

    for v in vendors:
        name = v["vendor"]
        handle = vendor_handle(name)
        if handle in {"none", "unknown", ""}:
            continue
        hit = resolve_bihr(handle, idx)
        if not hit:
            missing.append(
                {
                    "handle": handle,
                    "vendor": name,
                    "active_products": v.get("active_products", 0),
                }
            )
            continue
        matched.append(
            {
                "handle": handle,
                "vendor": name,
                "bihr_slug": hit["slug"],
                "bihr_name": hit["name"],
                "active_products": v.get("active_products", 0),
            }
        )

    if args.limit:
        matched = sorted(matched, key=lambda x: -x["active_products"])[: args.limit]

    for m in sorted(matched, key=lambda x: -x["active_products"]):
        src = RAW / f"{m['bihr_slug']}.png"
        dest = BATCH / f"merk-{m['handle']}.png"
        if not src.exists():
            copy_fail.append({**m, "error": f"raw missing: {src.name}"})
            continue
        try:
            shutil.copy2(src, dest)
            copied.append(m["handle"])
        except Exception as e:
            copy_fail.append({**m, "error": str(e)})

    sheets = build_contact_sheets(BATCH)

    uploaded: list[dict] = []
    if args.upload:
        # Prefer Motox credentials over any already-exported KTM shop env.
        env_motox = ROOT / ".env.motox"
        if env_motox.exists():
            for line in env_motox.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k in {
                    "SHOPIFY_ACCESS_TOKEN",
                    "SHOPIFY_SHOP_DOMAIN",
                    "SHOPIFY_ADMIN_API_VERSION",
                }:
                    os.environ[k] = v
        sys.path.insert(0, str(ROOT))
        import importlib

        import config as _cfg

        importlib.reload(_cfg)
        import modules.image_manager as im

        importlib.reload(im)

        files = sorted(BATCH.glob("merk-*.png"))
        print(f"Uploading {len(files)} logos to Motox Files…")
        for path in files:
            ok, url = im.upload_image(path)
            uploaded.append({"file": path.name, "ok": ok, "url": url})
            print(f"  {'OK' if ok else 'FAIL'} {path.name} → {url or '—'}")

    report = {
        "mode": "1to1-bihr",
        "source": "https://www.mybihr.com/ww/en/all-brands",
        "bihr_brand_count": len(items),
        "raw_ok": len(raw_ok),
        "raw_fail": raw_fail,
        "motox_matched": len(matched),
        "copied": copied,
        "copy_fail": copy_fail,
        "missing_motox_vendors": sorted(missing, key=lambda x: -x["active_products"]),
        "contact_sheets": [str(p.relative_to(ROOT)) for p in sheets],
        "uploaded": uploaded,
    }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(
        f"\nDone: {len(raw_ok)} Bihr raw, {len(copied)} Motox merk-*.png"
        f", {len(missing)} Motox vendors not on Bihr"
    )
    print(f"Report: {REPORT}")
    return 0 if copied else 1


if __name__ == "__main__":
    raise SystemExit(main())
