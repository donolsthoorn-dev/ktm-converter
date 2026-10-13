#!/usr/bin/env python3
"""
Maak opnieuw de Shopify delta-CSV’s voor KTM, HSQ en WP.

Alleen Image Src-URL’s die publiek een echt bestand teruggeven komen in de CSV.
Dode CDN-gokken en de placeholder fallback_no_picture blijven eruit.
Lokale bestanden onder input/ (en input/hsq, input/wp) worden indien nodig
naar Shopify geüpload en daarna als URL in de CSV gezet.

  cd ~/Documents/ktm_project
  python3 -u scripts/rebuild_shopify_deltas.py

Alleen één of twee merken:

  python3 -u scripts/rebuild_shopify_deltas.py --brands hsq,wp
  python3 -u scripts/rebuild_shopify_deltas.py --brands ktm

Daarna in Shopify Admin per merk de nieuwste shopify_export_delta_*.csv importeren
(bestaande handles niet overschrijven).
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

DEFAULT_BRANDS = ("ktm", "hsq", "wp")

_PRODUCTS_DIR = {
    "ktm": ROOT / "output" / "products",
    "hsq": ROOT / "output" / "hsq" / "products",
    "wp": ROOT / "output" / "wp" / "products",
}


def _latest_delta(brand: str) -> Path | None:
    folder = _PRODUCTS_DIR[brand]
    paths = glob.glob(str(folder / "shopify_export_delta_*.csv"))
    if not paths:
        return None
    return Path(max(paths, key=os.path.getmtime))


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from modules.brand_config import VALID_BRAND_IDS, normalize_brand_id

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--brands",
        default=",".join(DEFAULT_BRANDS),
        help=f"Komma-gescheiden (default: {','.join(DEFAULT_BRANDS)})",
    )
    args = p.parse_args()

    brands = [normalize_brand_id(b.strip()) for b in args.brands.split(",") if b.strip()]
    for b in brands:
        if b not in VALID_BRAND_IDS:
            print(f"Onbekend merk: {b}", file=sys.stderr)
            return 1

    print(
        "Delta-CSV’s opnieuw maken. Alleen bereikbare productfoto’s in Image Src.\n"
        "Dit kan per merk 15–60+ minuten duren (afbeeldingen + CSV-schrijven).\n",
        flush=True,
    )

    failed = False
    deltas: list[tuple[str, Path | None]] = []
    for brand_id in brands:
        cmd = [sys.executable, "-u", str(ROOT / "main.py")]
        if brand_id != "ktm":
            cmd.extend(["--brand", brand_id])
        print(f"\n=== {brand_id} ===", flush=True)
        print(" ".join(cmd), flush=True)
        t0 = time.time()
        proc = subprocess.run(cmd, cwd=ROOT)
        if proc.returncode != 0:
            print(f"mislukt (exit {proc.returncode})", file=sys.stderr)
            failed = True
            deltas.append((brand_id, None))
            continue
        delta = _latest_delta(brand_id)
        deltas.append((brand_id, delta))
        mins = (time.time() - t0) / 60.0
        if delta:
            mb = delta.stat().st_size / (1024 * 1024)
            print(f"Klaar in {mins:.1f} min → {delta} ({mb:.1f} MB)", flush=True)
        else:
            print(f"Klaar in {mins:.1f} min, maar geen delta-CSV gevonden.", flush=True)

    print("\n=== Importeer in Shopify Admin ===", flush=True)
    for brand_id, delta in deltas:
        if delta:
            print(f"  {brand_id}: {delta}", flush=True)
        else:
            print(f"  {brand_id}: (geen bestand)", flush=True)
    print(
        "\nTip: bestaande handles niet overschrijven. "
        "Producten zonder werkende foto (geen lokaal/CDN-bestand, of >20 MB) "
        "staan niet in de delta.",
        flush=True,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
