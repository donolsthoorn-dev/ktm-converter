#!/usr/bin/env python3
"""
Pilot: pas één Kutvek seatcover-survivor aan tot samengevoegd product
(titel = design, opties Model + Model jaar, alle groeps-SKU's, YMM),
zonder sibling-producten te archiveren.

Voorbeeld (groep 35167614 / White Seat Cover 3 Ribs):
  python3 scripts/ktm_seatcover_merge_pilot.py --group 35167614 --apply
  python3 scripts/ktm_seatcover_merge_pilot.py --group 35167614   # dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util  # noqa: E402

import config  # noqa: E402
from modules.metafields_manager_export import (  # noqa: E402
    _pipe_join_sorted,
    _ymm_summary,
    _ymm_tuples_to_fits_on_json,
)

_spec = importlib.util.spec_from_file_location(
    "ktm_seatcover_ymm_dry_run",
    PROJECT_ROOT / "scripts" / "ktm_seatcover_ymm_dry_run.py",
)
_dry = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_dry)
normalize_kutvek_model = _dry.normalize_kutvek_model
normalize_make = _dry.normalize_make
years_from_band = _dry.years_from_band

DEFAULT_RAW = PROJECT_ROOT / "output" / "seatcovers_raw.jsonl"
TITLE_RE = re.compile(
    r"^(?P<head>.+?)\s*-\s*(?P<make>KTM|HUSQVARNA|HUSKY|GASGAS|GAS\s*GAS)\s+"
    r"(?P<model>.+?)\s*\((?P<y1>\d{4})\s*[-–]\s*(?P<y2>\d{4})\)\s*$",
    re.I,
)


def load_dotenv(path: Path | None = None) -> None:
    path = path or (PROJECT_ROOT / ".env")
    if not path.is_file():
        return
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                if not key:
                    continue
                val = val.strip()
                if (val.startswith('"') and val.endswith('"')) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                if key not in os.environ:
                    os.environ[key] = val
    except OSError:
        return


def digits(s: str) -> str:
    return re.sub(r"[^0-9]", "", s or "")


def design_key(sku: str) -> str:
    d = digits(sku)
    return d[:-3] if len(d) >= 11 else ""


def session_and_url() -> tuple[requests.Session, str, dict]:
    load_dotenv()
    shop = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN or "").strip()
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or config.SHOPIFY_ACCESS_TOKEN or "").strip()
    ver = (
        os.environ.get("SHOPIFY_ADMIN_API_VERSION")
        or config.SHOPIFY_ADMIN_API_VERSION
        or "2024-10"
    ).strip()
    if not shop or not token:
        raise SystemExit("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt.")
    sess = requests.Session()
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    url = f"https://{shop}/admin/api/{ver}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    return sess, url, headers


def gql(sess: requests.Session, url: str, headers: dict, query: str, variables: dict) -> dict:
    for i in range(8):
        r = sess.post(
            url, headers=headers, json={"query": query, "variables": variables}, timeout=120
        )
        if r.status_code == 429:
            time.sleep(2 + i)
            continue
        r.raise_for_status()
        body = r.json()
        errors = body.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors):
            time.sleep(2 + i)
            continue
        if errors:
            raise RuntimeError(json.dumps(errors, indent=2)[:2000])
        return body.get("data") or {}
    raise RuntimeError("te vaak throttled")


def load_group_nodes(raw_path: Path, group_id: str) -> tuple[list[dict], str]:
    nodes = [json.loads(l) for l in raw_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    matched: list[dict] = []
    title_head = ""
    for n in nodes:
        title = re.sub(r"\s+", " ", (n.get("title") or "")).strip()
        m = TITLE_RE.match(title)
        if not m:
            continue
        skus = [
            (v.get("sku") or "").strip()
            for v in ((n.get("variants") or {}).get("nodes") or [])
            if (v.get("sku") or "").strip()
        ]
        keys = [design_key(s) for s in skus if design_key(s)]
        if not keys:
            continue
        dkey = max(set(keys), key=keys.count)
        if dkey != group_id:
            continue
        matched.append(n)
        title_head = re.sub(r"\s+", " ", m.group("head")).strip()
    if not matched:
        raise SystemExit(f"Geen producten voor group {group_id} in {raw_path}")
    return matched, title_head


def pick_survivor(nodes: list[dict], preferred_handle: str | None) -> dict:
    if preferred_handle:
        for n in nodes:
            if (n.get("handle") or "") == preferred_handle:
                return n
    # meeste varianten, anders laagste id
    def score(n: dict) -> tuple:
        nvar = len(((n.get("variants") or {}).get("nodes") or []))
        pid = int(re.search(r"/(\d+)$", n.get("id") or "").group(1))
        return (nvar, -pid)

    return max(nodes, key=score)


def build_variant_plan(nodes: list[dict]) -> tuple[list[dict], set[tuple[str, str, str]]]:
    """Unieke (model, band) → sku/price + YMM tuples."""
    by_key: dict[tuple[str, str], dict] = {}
    tuples: set[tuple[str, str, str]] = set()
    for n in nodes:
        title = re.sub(r"\s+", " ", (n.get("title") or "")).strip()
        m = TITLE_RE.match(title)
        if not m:
            continue
        make = normalize_make(m.group("make"))
        model_raw = m.group("model").strip()
        model_norm, _ = normalize_kutvek_model(model_raw)
        y1, y2 = int(m.group("y1")), int(m.group("y2"))
        for v in ((n.get("variants") or {}).get("nodes") or []):
            band = (v.get("title") or "").strip()
            sku = (v.get("sku") or "").strip()
            price = (v.get("price") or "").strip() or "0.00"
            if not band or band.lower() == "default title" or not sku:
                continue
            key = (model_raw, band)
            # voorkeur: 11-cijferige SKU boven verminkte 10-cijferige
            prev = by_key.get(key)
            if prev is None or (len(digits(sku)) >= 11 and len(digits(prev["sku"])) < 11):
                by_key[key] = {
                    "model": model_raw,
                    "band": band,
                    "sku": sku,
                    "price": price,
                }
            for y in years_from_band(band, y1, y2):
                tuples.add((make, model_norm or model_raw.upper(), y))
    variants = sorted(by_key.values(), key=lambda r: (r["model"], r["band"], r["sku"]))
    return variants, tuples


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description="Pilot merge één seatcover-survivor")
    ap.add_argument("--group", required=True, help="Design group id, bv. 35167614")
    ap.add_argument("--survivor-handle", default="35167614174-3190")
    ap.add_argument("--raw", type=Path, default=DEFAULT_RAW)
    ap.add_argument("--apply", action="store_true", help="Echt schrijven naar Shopify")
    args = ap.parse_args()

    nodes, title_head = load_group_nodes(args.raw, args.group)
    survivor = pick_survivor(nodes, args.survivor_handle)
    variants, tuples = build_variant_plan(nodes)
    new_title = title_head
    fits_on = _ymm_tuples_to_fits_on_json(tuples)
    ymm_summary = (_ymm_summary(tuples) or "")[:255]
    product_id = survivor["id"]

    models = sorted({v["model"] for v in variants})
    bands = sorted({v["band"] for v in variants})

    print(f"Group:           {args.group}")
    print(f"Siblings in raw: {len(nodes)} (blijven staan)")
    print(f"Survivor:        {survivor.get('handle')} ({product_id})")
    print(f"Title:           {survivor.get('title')!r} → {new_title!r}")
    print(f"Variants:        {len(((survivor.get('variants') or {}).get('nodes') or []))} → {len(variants)}")
    print(f"Models:          {len(models)}")
    print(f"Year bands:      {len(bands)}")
    print(f"ymm_summary:     {ymm_summary}")
    print(f"fits_on years:   {len({y for _, _, y in tuples})} unique years, {len(tuples)} tuples")
    print(f"fits_on_make:    {_pipe_join_sorted({m for m, _, _ in tuples})}")

    plan_path = PROJECT_ROOT / "output" / "seatcovers" / f"merge_pilot_{args.group}_plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(
        json.dumps(
            {
                "group": args.group,
                "survivor_handle": survivor.get("handle"),
                "survivor_id": product_id,
                "new_title": new_title,
                "ymm_summary": ymm_summary,
                "fits_on": json.loads(fits_on),
                "variants": variants,
                "sibling_handles": sorted(n.get("handle") or "" for n in nodes),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"Plan:            {plan_path}")

    if not args.apply:
        print("Dry-run — geen Shopify-writes. Gebruik --apply om door te zetten.")
        return 0

    sess, url, headers = session_and_url()

    product_options = [
        {
            "name": "Model",
            "position": 1,
            "values": [{"name": m} for m in models],
        },
        {
            "name": "Model jaar",
            "position": 2,
            "values": [{"name": b} for b in bands],
        },
    ]
    variant_inputs = []
    for v in variants:
        variant_inputs.append(
            {
                "optionValues": [
                    {"optionName": "Model", "name": v["model"]},
                    {"optionName": "Model jaar", "name": v["band"]},
                ],
                "sku": v["sku"],
                "price": v["price"],
                "inventoryPolicy": "DENY",
            }
        )

    mutation = """
    mutation pilotMerge($input: ProductSetInput!, $synchronous: Boolean!) {
      productSet(synchronous: $synchronous, input: $input) {
        product {
          id
          title
          handle
          options { name values }
          variants(first: 250) {
            nodes { id sku title selectedOptions { name value } }
          }
        }
        productSetOperation { id status }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "synchronous": True,
        "input": {
            "id": product_id,
            "title": new_title,
            "productOptions": product_options,
            "variants": variant_inputs,
        },
    }
    print(f"Shopify productSet ({len(variant_inputs)} variants)…", flush=True)
    data = gql(sess, url, headers, mutation, variables)
    payload = data.get("productSet") or {}
    errs = payload.get("userErrors") or []
    if errs:
        print("productSet userErrors:", json.dumps(errs, indent=2), file=sys.stderr)
        return 1
    product = payload.get("product") or {}
    print(f"OK title={product.get('title')!r} variants={len((product.get('variants') or {}).get('nodes') or [])}")

    meta_mutation = """
    mutation setYmm($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { namespace key value }
        userErrors { field message code }
      }
    }
    """
    meta_vars = {
        "metafields": [
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on",
                "type": "json",
                "value": fits_on,
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "ymm_summary",
                "type": "single_line_text_field",
                "value": ymm_summary,
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on_make",
                "type": "single_line_text_field",
                "value": _pipe_join_sorted({m for m, _, _ in tuples}),
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on_model",
                "type": "single_line_text_field",
                "value": _pipe_join_sorted({md for _, md, _ in tuples}),
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on_year",
                "type": "single_line_text_field",
                "value": _pipe_join_sorted({y for _, _, y in tuples}),
            },
        ]
    }
    print("Shopify metafieldsSet…", flush=True)
    mdata = gql(sess, url, headers, meta_mutation, meta_vars)
    mpayload = mdata.get("metafieldsSet") or {}
    merrs = mpayload.get("userErrors") or []
    if merrs:
        print("metafieldsSet userErrors:", json.dumps(merrs, indent=2), file=sys.stderr)
        return 1
    print("YMM metafields gezet.")
    print(f"Controleer: https://www.ktm-shop.nl/products/{survivor.get('handle')}")
    print("Duplicaten niet aangeraakt.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
