#!/usr/bin/env python3
"""Upload ALL Bihr brand logos to Motox Files as merk-{slug}.png.

Also keeps Motox-only handle aliases from batch20 (e.g. merk-haan.png → same
asset as haan-wheels) so existing vendorHandles keep working.
"""
from __future__ import annotations

import importlib
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "output" / "merk-logos" / "bihr-raw"
BATCH = ROOT / "output" / "merk-logos" / "batch20"
STAGING = ROOT / "output" / "merk-logos" / "bihr-all-merk"
REPORT = BATCH / "upload-results-bihr-all.json"


def load_motox_env() -> None:
    env_path = ROOT / ".env.motox"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ[k.strip()] = v.strip().strip('"').strip("'")


def main() -> int:
    if not RAW.exists():
        raise SystemExit(f"missing {RAW}")

    load_motox_env()
    sys.path.insert(0, str(ROOT))
    import config

    importlib.reload(config)
    import modules.image_manager as im

    importlib.reload(im)
    im._CDN_POLL_ATTEMPTS = 6
    im._CDN_POLL_SLEEP_SEC = 0.35

    if STAGING.exists():
        shutil.rmtree(STAGING)
    STAGING.mkdir(parents=True)

    # 1) Every Bihr slug → merk-{slug}.png
    for src in sorted(RAW.glob("*.png")):
        shutil.copy2(src, STAGING / f"merk-{src.stem}.png")

    # 2) Motox alias handles not equal to a Bihr slug
    bihr_slugs = {p.stem for p in RAW.glob("*.png")}
    for src in sorted(BATCH.glob("merk-*.png")):
        handle = src.stem[len("merk-") :]
        if handle not in bihr_slugs:
            shutil.copy2(src, STAGING / src.name)

    files = sorted(STAGING.glob("merk-*.png"))
    print(f"shop: {config.SHOPIFY_SHOP_DOMAIN}")
    print(f"staging: {len(files)} files (Bihr all + Motox aliases)")
    print("mode: REPLACE")

    results: list[dict] = []
    ok_n = fail_n = 0
    t0 = time.time()

    for i, path in enumerate(files, 1):
        ok = False
        url = None
        err = None
        for attempt in range(1, 4):
            try:
                ok, url = im.upload_image(path)
            except Exception as e:  # noqa: BLE001
                ok, url, err = False, None, str(e)
            if ok and url:
                break
            time.sleep(0.5 * attempt)
        if ok and url:
            ok_n += 1
            mark = "OK"
        else:
            fail_n += 1
            mark = "FAIL"
        results.append({"file": path.name, "ok": bool(ok and url), "url": url, "error": err})
        print(f"[{i}/{len(files)}] {mark} {path.name}", flush=True)
        time.sleep(0.2)

    REPORT.write_text(
        json.dumps(
            {
                "shop": config.SHOPIFY_SHOP_DOMAIN,
                "mode": "REPLACE",
                "ok": ok_n,
                "fail": fail_n,
                "seconds": round(time.time() - t0, 1),
                "results": results,
            },
            indent=2,
        )
    )
    print(f"\nDone: {ok_n} ok, {fail_n} fail in {time.time() - t0:.0f}s")
    print(f"Report: {REPORT}")
    return 0 if fail_n == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
