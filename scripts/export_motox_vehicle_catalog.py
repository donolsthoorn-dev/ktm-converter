"""Bouw de Motox-motorlijst uit global.fits_on van ktm-shop-nederland.

Dat is het Motox-assortiment met fitment (e-Bihr, motofilter): Honda, Yamaha,
Kawasaki en de rest. De OEM-tabel canonical_product_fits_on is alleen
KTM/Husqvarna/GasGas en hoort hier niet in. POS-kopieën zonder fits_on
leveren geen merk op.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.motox_garage.catalog import add_fits_on, catalog_from_tree, catalog_path  # noqa: E402
from modules.motox_shopify_cache import (  # noqa: E402
    _GQL_BULK_START,
    _GQL_POLL,
    _TIMEOUT,
    _gql,
    _session,
    _wait_current_bulk,
    load_motox_env,
)

STOREFRONT = ROOT / "motox" / "garage" / "extensions" / "motor-garage" / "assets" / "vehicles.json"
BULK_FILE = ROOT / "cache" / "motox" / "fits_on_bulk.jsonl"

_FITS_QUERY = """{
  products {
    edges {
      node {
        metafield(namespace: "global", key: "fits_on") {
          value
        }
      }
    }
  }
}"""


def _as_dict(value: object) -> dict | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if isinstance(value, dict) and value:
        return value
    return None


def fetch_fits_on(dest: Path) -> int:
    domain, token, api = load_motox_env()
    sess = _session()
    url = f"https://{domain}/admin/api/{api}/graphql.json"
    _wait_current_bulk(sess, url, token)
    query = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_FITS_QUERY))
    body = _gql(sess, url, token, query)
    if body.get("errors"):
        raise SystemExit(f"GraphQL start-fout: {body.get('errors')}")
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    if run.get("userErrors"):
        raise SystemExit(f"Bulk geweigerd: {run.get('userErrors')}")
    op_id = (run.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise SystemExit("Geen bulk operation id.")
    print("Motox fits_on-export gestart...", flush=True)
    while True:
        poll = _gql(sess, url, token, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            download = node.get("url")
            if not download:
                raise SystemExit("Bulk klaar zonder URL.")
            dest.parent.mkdir(parents=True, exist_ok=True)
            with sess.get(download, stream=True, timeout=_TIMEOUT, proxies={"http": None, "https": None}) as response:
                response.raise_for_status()
                with dest.open("wb") as handle:
                    for chunk in response.iter_content(1024 * 256):
                        if chunk:
                            handle.write(chunk)
            return dest.stat().st_size
        if status in ("FAILED", "CANCELED"):
            raise SystemExit(f"Bulk {status}: {node.get('errorCode')}")
        import time

        time.sleep(3)


def tree_from_bulk(path: Path) -> tuple[dict, int]:
    tree: dict = {}
    used = 0
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            raw = obj.get("metafield")
            value = raw.get("value") if isinstance(raw, dict) else obj.get("value")
            parsed = _as_dict(value)
            if not parsed:
                continue
            add_fits_on(tree, parsed)
            used += 1
    return tree, used


def write_catalog(catalog: dict, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(catalog, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    STOREFRONT.write_text(
        json.dumps(catalog, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", type=Path, default=catalog_path())
    parser.add_argument("--from-file", type=Path, default=None, help="Bestaande bulk-JSONL, zonder nieuwe Shopify-export")
    args = parser.parse_args()
    source = args.from_file
    if source is None:
        print(f"download → {BULK_FILE}", flush=True)
        fetch_fits_on(BULK_FILE)
        source = BULK_FILE
    tree, used = tree_from_bulk(source)
    catalog = catalog_from_tree(tree)
    write_catalog(catalog, args.o)
    models = sum(len(make["models"]) for make in catalog["makes"])
    names = ", ".join(make["make"] for make in catalog["makes"][:12])
    print(f"{used} producten met fits_on", flush=True)
    print(f"{len(catalog['makes'])} merken, {models} modellen → {args.o}", flush=True)
    print(names, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
