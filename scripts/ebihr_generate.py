#!/usr/bin/env python3
"""
End-to-end e-bihr generate: raw → valid-products + ymm-filter + ymm-metafields
(+ optioneel Motox new-only / Shopify-ID exports).

  # Alleen bouwen vanaf bestaande raw:
  python3 scripts/ebihr_generate.py --raw-dir motox/e-bihr/raw/<ts>

  # Fetch + build:
  python3 scripts/ebihr_generate.py --fetch

  # Inclusief Motox new-only CSV's (gebruikt cache/motox):
  python3 scripts/ebihr_generate.py --raw-dir ... --motox-ready
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.build_products import build_product_csv_rows  # noqa: E402
from modules.ebihr.build_ymm import (  # noqa: E402
    build_ymm_from_raw,
    fits_on_rows,
    ymm_filter_rows,
)
from modules.ebihr.client import BihrClient  # noqa: E402
from modules.ebihr.constants import (  # noqa: E402
    CSV_HEADER,
    DEFAULT_CHUNK_BYTES,
    EBIHR_ROOT,
    GENERATED_ROOT,
    RAW_ROOT,
    YMM_CSV_HEADER,
)
from modules.ebihr.csv_util import split_csv_by_bytes, write_csv  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ebihr.generate")


def _latest_raw_dir() -> Path | None:
    if not RAW_ROOT.is_dir():
        return None
    dirs = [p for p in RAW_ROOT.iterdir() if p.is_dir() and (p / "FETCH_OK").is_file()]
    if not dirs:
        dirs = [p for p in RAW_ROOT.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)


def _run_motox_ready(products_dir: Path) -> None:
    """Invoke existing Motox new-only + (if cache present) prepare scripts."""
    from scripts.motox_ebihr_filter_new_products import main as filter_main

    # filter script uses argparse via main — call as subprocess-like via argv
    old = sys.argv[:]
    try:
        sys.argv = [
            "motox_ebihr_filter_new_products.py",
            "--products-dir",
            str(products_dir),
        ]
        code = filter_main()
        if code not in (0, None):
            raise SystemExit(code)
    finally:
        sys.argv = old

    # Prepare YMM/metafields only if Motox cache exists (product IDs).
    cache_idx = ROOT / "cache" / "motox" / "shopify_products_index.json"
    if not cache_idx.is_file():
        log.warning(
            "Motox-cache ontbreekt; sla prepare_ymm_metafields over. "
            "Draai na productimport: python3 scripts/motox_ebihr_prepare_ymm_metafields.py --refresh-cache"
        )
        return
    from scripts.motox_ebihr_prepare_ymm_metafields import main as prep_main

    old = sys.argv[:]
    try:
        sys.argv = ["motox_ebihr_prepare_ymm_metafields.py"]
        code = prep_main()
        # exit 2 = nog geen gemapte producten (verwacht vóór import)
        if code not in (0, 2, None):
            raise SystemExit(code)
    finally:
        sys.argv = old


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate e-bihr Shopify/YMM CSVs.")
    ap.add_argument("--fetch", action="store_true", help="Eerst V3+VSE ophalen.")
    ap.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Raw map (default: nieuwste motox/e-bihr/raw/*).",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Outputmap (default: motox/e-bihr/generated/<ts>).",
    )
    ap.add_argument(
        "--max-mb",
        type=float,
        default=14.0,
        help="Chunk-limiet MB (default: 14).",
    )
    ap.add_argument(
        "--motox-ready",
        action="store_true",
        help="Na generate: Motox new-only filter (+ prepare als cache er is).",
    )
    ap.add_argument(
        "--skip-products",
        action="store_true",
        help="Alleen YMM bouwen.",
    )
    ap.add_argument(
        "--skip-ymm",
        action="store_true",
        help="Alleen products bouwen.",
    )
    args = ap.parse_args()

    raw_dir = args.raw_dir
    if args.fetch:
        raw_dir = raw_dir or (RAW_ROOT / str(int(time.time())))
        BihrClient().fetch_all(raw_dir)
    if raw_dir is None:
        raw_dir = _latest_raw_dir()
    if raw_dir is None or not raw_dir.is_dir():
        raise SystemExit(
            "Geen raw-dir. Gebruik --fetch of --raw-dir motox/e-bihr/raw/<ts>."
        )

    ts = str(int(time.time()))
    out_dir = args.out_dir or (GENERATED_ROOT / ts)
    out_dir.mkdir(parents=True, exist_ok=True)
    max_bytes = int(args.max_mb * 1024 * 1024)

    ymm_data = None
    if not args.skip_ymm or not args.skip_products:
        # products need offroad set from YMM/VSE
        try:
            ymm_data = build_ymm_from_raw(raw_dir)
        except FileNotFoundError as e:
            if args.skip_ymm:
                log.warning("YMM/VSE ontbreekt (%s); hardpart-filter zonder offroad-set.", e)
                ymm_data = {
                    "ymm": {},
                    "fits_on": {},
                    "offroad_partnumbers": set(),
                }
            else:
                raise

    if not args.skip_ymm and ymm_data is not None:
        ymm_path = out_dir / f"ymm-filter-data-ebihr-{ts}.csv"
        write_csv(ymm_path, YMM_CSV_HEADER, ymm_filter_rows(ymm_data["ymm"]))
        # also copy to e-bihr root for Motox scripts that glob daar
        shutil.copy2(ymm_path, EBIHR_ROOT / ymm_path.name)

        meta_path = out_dir / f"ymm-metafields-data-ebihr-{ts}.csv"
        write_csv(meta_path, ["handle", "fits_on"], fits_on_rows(ymm_data["fits_on"]))
        meta_parts = split_csv_by_bytes(meta_path, max_bytes=max_bytes)
        # mirror chunks under e-bihr root with familiar name
        root_chunks = EBIHR_ROOT / f"ymm-metafields-data-ebihr-{ts}_chunks"
        if root_chunks.exists():
            shutil.rmtree(root_chunks)
        if len(meta_parts) == 1 and meta_parts[0] == meta_path:
            root_chunks.mkdir(parents=True)
            shutil.copy2(meta_path, root_chunks / f"ymm-metafields-data-ebihr-{ts}_part1.csv")
        else:
            shutil.copytree(meta_path.with_name(meta_path.stem + "_chunks"), root_chunks)

    products_chunks_dir = None
    if not args.skip_products:
        offroad = (ymm_data or {}).get("offroad_partnumbers") or set()
        rows, stats = build_product_csv_rows(raw_dir, offroad_partnumbers=set(offroad))
        (out_dir / "report_products_stats.json").write_text(
            __import__("json").dumps(stats, indent=2), encoding="utf-8"
        )
        prod_path = out_dir / f"valid-products-ebihr-{ts}.csv"
        write_csv(prod_path, CSV_HEADER, rows)
        parts = split_csv_by_bytes(prod_path, max_bytes=max_bytes)
        products_chunks_dir = EBIHR_ROOT / f"valid-products-ebihr-{ts}_chunks"
        if products_chunks_dir.exists():
            shutil.rmtree(products_chunks_dir)
        if len(parts) == 1 and parts[0] == prod_path:
            products_chunks_dir.mkdir(parents=True)
            shutil.copy2(prod_path, products_chunks_dir / f"valid-products-ebihr-{ts}_part1.csv")
        else:
            shutil.copytree(prod_path.with_name(prod_path.stem + "_chunks"), products_chunks_dir)
        log.info("Product chunks: %s", products_chunks_dir)

    print(f"Generated → {out_dir}")
    if products_chunks_dir:
        print(f"Products chunks → {products_chunks_dir}")

    if args.motox_ready:
        if not products_chunks_dir:
            raise SystemExit("--motox-ready vereist product-output (geen --skip-products).")
        _run_motox_ready(products_chunks_dir)
        print("Motox-ready outputs → motox/e-bihr/output/")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
