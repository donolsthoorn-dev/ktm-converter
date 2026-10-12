"""Bouw de KTM-motorlijst uit canonical_product_fits_on (Supabase).

Alleen OEM: KTM, Husqvarna, GASGAS. Bron is de XML-pipeline naar Shopify,
niet de Motox aftermarket-fits_on.
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
from modules.motox_garage.catalog import add_fits_on, catalog_from_tree  # noqa: E402
from modules.motox_garage.model import clean_token  # noqa: E402

OUT = ROOT / "ktm" / "garage" / "catalog" / "vehicles.json"
STOREFRONT = ROOT / "ktm" / "garage" / "extensions" / "motor-garage" / "assets" / "vehicles.json"
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
        key = clean_token(make).upper()
        if key not in OEM_MAKES:
            continue
        if isinstance(models, dict) and models:
            out[make] = models
    return out


def iter_ymm_json(sess: requests.Session) -> list[dict]:
    base = _base()
    headers = _headers()
    page = 1000
    start = 0
    rows: list[dict] = []
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
        rows.extend(chunk)
        if len(chunk) < page:
            break
        start += page
        print(f"  gelezen {len(rows)}...", flush=True)
    return rows


def main() -> int:
    load_project_env()
    sess = _session()
    print("Supabase canonical_product_fits_on → KTM-catalogus...", flush=True)
    rows = iter_ymm_json(sess)
    tree: dict = {}
    used = 0
    for row in rows:
        raw = row.get("ymm_json")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                continue
        if not isinstance(raw, dict) or not raw:
            continue
        filtered = oem_only(raw)
        if not filtered:
            continue
        add_fits_on(tree, filtered)
        used += 1
    catalog = catalog_from_tree(tree)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(catalog, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    STOREFRONT.parent.mkdir(parents=True, exist_ok=True)
    STOREFRONT.write_text(
        json.dumps(catalog, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    models = sum(len(make["models"]) for make in catalog["makes"])
    names = ", ".join(make["make"] for make in catalog["makes"])
    print(f"{used} producten met OEM-fitment", flush=True)
    print(f"{len(catalog['makes'])} merken, {models} modellen → {OUT}", flush=True)
    print(names, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
