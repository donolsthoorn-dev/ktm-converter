"""Bouw de KTM fitment-index uit canonical_product_fits_on (Supabase).

Geen POS-kassakopieën op ktm-shop.nl: producten zonder fitment blijven
universeel zichtbaar (niet in fitted → filter laat ze staan).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_project_env  # noqa: E402
from modules.motox_garage.fitment_index import FitmentIndex  # noqa: E402
from modules.motox_garage.model import clean_token  # noqa: E402

INDEX_FILE = ROOT / "cache" / "ktm" / "fitment_index.sqlite"
OEM_MAKES = frozenset({"KTM", "HUSQVARNA", "GASGAS"})


def _session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def _headers() -> dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        raise SystemExit("SUPABASE_SERVICE_ROLE_KEY ontbreekt")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _base() -> str:
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    if not url:
        raise SystemExit("SUPABASE_URL ontbreekt")
    return f"{url}/rest/v1"


def oem_only(fits_on: dict) -> dict:
    out: dict = {}
    for make, models in fits_on.items():
        if clean_token(make).upper() not in OEM_MAKES:
            continue
        if isinstance(models, dict) and models:
            out[make] = models
    return out


def iter_rows(sess: requests.Session):
    base = _base()
    headers = _headers()
    page = 1000
    start = 0
    while True:
        resp = sess.get(
            f"{base}/canonical_product_fits_on",
            headers={**headers, "Range": f"{start}-{start + page - 1}"},
            params={"select": "shopify_product_id,ymm_json", "ymm_json": "not.is.null"},
            timeout=(15, 120),
            proxies={"http": None, "https": None},
        )
        resp.raise_for_status()
        chunk = resp.json()
        if not chunk:
            break
        yield from chunk
        if len(chunk) < page:
            break
        start += page
        print(f"  gelezen tot {start}...", flush=True)


def main() -> int:
    import argparse

    load_project_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", type=Path, default=INDEX_FILE)
    args = parser.parse_args()

    print(f"Index → {args.o}", flush=True)
    index = FitmentIndex.create(args.o)
    counts = {"fitted": 0, "skipped": 0}
    pending = 0
    for row in iter_rows(_session()):
        product_id = row.get("shopify_product_id")
        try:
            product_id = int(product_id)
        except (TypeError, ValueError):
            counts["skipped"] += 1
            continue
        raw = row.get("ymm_json")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                counts["skipped"] += 1
                continue
        if not isinstance(raw, dict):
            counts["skipped"] += 1
            continue
        filtered = oem_only(raw)
        kind = index.add_product(product_id, [], filtered if filtered else None)
        if kind == "fitted":
            counts["fitted"] += 1
        else:
            counts["skipped"] += 1
        pending += 1
        if pending >= 2000:
            index.finish()
            pending = 0
    index.finish()
    stored = index.counts()
    print(
        f"{counts['fitted']} met fitment, {counts['skipped']} overgeslagen, "
        f"{stored['pairs']} motor-koppelingen → {args.o}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
