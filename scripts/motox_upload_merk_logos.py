#!/usr/bin/env python3
"""Upload batch20/merk-*.png to Motox Shopify Files with REPLACE + retries."""
from __future__ import annotations

import importlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BATCH = ROOT / "output" / "merk-logos" / "batch20"
REPORT = BATCH / "upload-results-bihr-1to1.json"


def load_motox_env() -> None:
    env_path = ROOT / ".env.motox"
    if not env_path.exists():
        raise SystemExit(f"missing {env_path}")
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ[k.strip()] = v.strip().strip('"').strip("'")


def main() -> int:
    load_motox_env()
    sys.path.insert(0, str(ROOT))
    import config

    importlib.reload(config)
    import modules.image_manager as im

    importlib.reload(im)

    # Faster CDN poll for small logo files
    im._CDN_POLL_ATTEMPTS = 6
    im._CDN_POLL_SLEEP_SEC = 0.4

    files = sorted(BATCH.glob("merk-*.png"))
    print(f"shop: {config.SHOPIFY_SHOP_DOMAIN}")
    print(f"upload {len(files)} files (duplicateResolutionMode=REPLACE, 3 retries)…")

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
            time.sleep(0.6 * attempt)
        if ok and url:
            ok_n += 1
            mark = "OK"
        else:
            fail_n += 1
            mark = "FAIL"
        results.append({"file": path.name, "ok": bool(ok and url), "url": url, "error": err})
        print(f"[{i}/{len(files)}] {mark} {path.name} → {url or '—'}", flush=True)
        time.sleep(0.25)

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
