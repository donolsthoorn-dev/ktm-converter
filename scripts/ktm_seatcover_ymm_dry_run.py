#!/usr/bin/env python3
"""
Dry-run: leid YMM-metafields af uit Kutvek seatcover-titels op ktm-shop.nl.

Leest `output/seatcovers_raw.jsonl` (of `--refresh` via Shopify GraphQL) en schrijft:
  - Metafields Manager CSV (komma) klaar voor review/import
  - Review-CSV (puntkomma) met parse-status, raw vs genormaliseerd model

Voorbeelden:
  python3 scripts/ktm_seatcover_ymm_dry_run.py
  python3 scripts/ktm_seatcover_ymm_dry_run.py --refresh
  python3 scripts/ktm_seatcover_ymm_dry_run.py --include-existing
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import defaultdict
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

import config  # noqa: E402
from modules.metafields_manager_export import (  # noqa: E402
    METAFIELDS_HEADER,
    _pipe_join_sorted,
    _upper_fits_on_json_cell,
    _upper_plain_metafield_cell,
    _ymm_summary,
    _ymm_tuples_to_fits_on_json,
)

DEFAULT_RAW = PROJECT_ROOT / "output" / "seatcovers_raw.jsonl"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output" / "seatcovers"

TITLE_RE = re.compile(
    r"^(?P<head>.+?)\s*-\s*(?P<make>KTM|HUSQVARNA|HUSKY|GASGAS|GAS\s*GAS)\s+"
    r"(?P<model>.+?)\s*\((?P<y1>\d{4})\s*[-–]\s*(?P<y2>\d{4})\)\s*$",
    re.I,
)

_YEAR_BAND_FULL = re.compile(r"^(\d{4})\s*[-–]\s*(\d{4})$")
_YEAR_BAND_OPEN = re.compile(r"^(\d{4})\s*[-–]\s*$")
_YEAR_ONLY = re.compile(r"^(\d{4})$")


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


def _numeric_product_id(gid_or_id: str) -> str:
    raw = (gid_or_id or "").strip()
    if raw.isdigit():
        return raw
    m = re.search(r"/Product/(\d+)$", raw)
    return m.group(1) if m else ""


def normalize_make(raw: str) -> str:
    u = re.sub(r"\s+", " ", (raw or "").strip()).upper()
    if u in {"HUSKY", "HUSQVARNA"}:
        return "HUSQVARNA"
    if u in {"GAS GAS", "GASGAS"}:
        return "GASGAS"
    return u or "KTM"


def normalize_kutvek_model(raw: str) -> tuple[str, str]:
    """
    Zet Kutvek-titelmodel om naar catalogus-achtige vorm (cc vóór lijn).

    Returns (normalized, note). note leeg als conversie schoon was.
    """
    s = re.sub(r"\s+", " ", (raw or "").strip()).upper()
    if not s:
        return "", "empty_model"

    # Specials zonder cc in de titel / afwijkende vormen
    specials = {
        "XC F-W": ("XCF-W", "no_cc_in_title"),
        "XC W": ("XC-W", "no_cc_in_title"),
        "EXC EXCF": ("EXC/EXC-F", "ambiguous_model"),
        "SX E 3": ("SX-E 3", ""),
        "SX E 5": ("SX-E 5", ""),
    }
    if s in specials:
        return specials[s]

    # LINE CC [rest]  →  CC LINE[-F] [rest]
    m = re.match(
        r"^(SX|EXC|XC|SMR|SMC|ENDURO)\s+(\d{2,4})(?:\s+(.*))?$",
        s,
    )
    if not m:
        return s, "unmapped_shape"

    line, cc, rest = m.group(1), m.group(2), (m.group(3) or "").strip()

    if line == "SX":
        if rest == "F" or rest.startswith("F "):
            suffix = rest[1:].strip()
            model = f"{cc} SX-F"
            return (f"{model} {suffix}".strip(), "")
        if rest:
            return f"{cc} SX {rest}".strip(), ""
        return f"{cc} SX", ""

    if line == "EXC":
        if rest == "F" or rest.startswith("F "):
            suffix = rest[1:].strip()
            model = f"{cc} EXC-F"
            return (f"{model} {suffix}".strip(), "")
        if rest:
            # TPI, SIX DAYS, …
            return f"{cc} EXC {rest}".strip(), ""
        return f"{cc} EXC", ""

    if line == "XC":
        if rest == "F" or rest.startswith("F "):
            suffix = rest[1:].strip()
            model = f"{cc} XC-F"
            return (f"{model} {suffix}".strip(), "")
        if rest in {"W", "W TPI"} or rest.startswith("W "):
            suffix = rest[1:].strip()
            model = f"{cc} XC-W"
            return (f"{model} {suffix}".strip(), "")
        if rest:
            return f"{cc} XC {rest}".strip(), ""
        return f"{cc} XC", ""

    if line in {"SMR", "SMC", "ENDURO"}:
        if rest:
            return f"{cc} {line} {rest}".strip(), ""
        return f"{cc} {line}", ""

    return s, "unmapped_shape"


def years_from_band(
    band: str, title_y1: int | None, title_y2: int | None
) -> list[str]:
    band = re.sub(r"\s+", " ", (band or "").strip())
    m = _YEAR_BAND_FULL.match(band)
    if m:
        y1, y2 = int(m.group(1)), int(m.group(2))
        if y2 < y1:
            y1, y2 = y2, y1
        return [str(y) for y in range(y1, y2 + 1)]
    m = _YEAR_BAND_OPEN.match(band)
    if m:
        y1 = int(m.group(1))
        if title_y2 is not None and title_y2 >= y1:
            return [str(y) for y in range(y1, title_y2 + 1)]
        return [str(y1)]
    m = _YEAR_ONLY.match(band)
    if m:
        return [m.group(1)]
    if title_y1 is not None and title_y2 is not None:
        y1, y2 = title_y1, title_y2
        if y2 < y1:
            y1, y2 = y2, y1
        return [str(y) for y in range(y1, y2 + 1)]
    return []


def tuples_from_product(node: dict) -> tuple[set[tuple[str, str, str]], dict]:
    """Bouw YMM-tuples + review-meta uit één Shopify product-node."""
    title = re.sub(r"\s+", " ", (node.get("title") or "")).strip()
    meta: dict = {
        "title": title,
        "parse_status": "",
        "model_raw": "",
        "model_norm": "",
        "model_note": "",
        "make": "",
        "year_source": "",
        "variant_bands": "",
    }
    m = TITLE_RE.match(title)
    if not m:
        meta["parse_status"] = "skip_title_unparsed"
        return set(), meta

    make = normalize_make(m.group("make"))
    model_raw = m.group("model").strip()
    model_norm, model_note = normalize_kutvek_model(model_raw)
    y1, y2 = int(m.group("y1")), int(m.group("y2"))
    meta.update(
        {
            "make": make,
            "model_raw": model_raw,
            "model_norm": model_norm,
            "model_note": model_note,
        }
    )

    variants = ((node.get("variants") or {}).get("nodes")) or []
    bands: list[str] = []
    years: set[str] = set()
    for v in variants:
        band = (v.get("title") or "").strip()
        # Default-title "Default Title" → gebruik producttitel-jaren
        if not band or band.lower() == "default title":
            continue
        bands.append(band)
        for y in years_from_band(band, y1, y2):
            years.add(y)

    if years:
        meta["year_source"] = "variant_bands"
        meta["variant_bands"] = " | ".join(bands)
    else:
        meta["year_source"] = "title_range"
        years = set(years_from_band(f"{y1}-{y2}", y1, y2))
        meta["variant_bands"] = f"{y1}-{y2}"

    if not years or not model_norm:
        meta["parse_status"] = "skip_no_years_or_model"
        return set(), meta

    tuples = {(make, model_norm, y) for y in years}
    meta["parse_status"] = "ok" if not model_note else f"ok_{model_note}"
    return tuples, meta


def metafields_row(
    product_id: str,
    handle: str,
    title: str,
    tuples: set[tuple[str, str, str]],
    mpn: str = "",
) -> list[str]:
    fits_on = _ymm_tuples_to_fits_on_json(tuples)
    years = {y for _, _, y in tuples}
    makes = {mk for mk, _, _ in tuples}
    models = {md for _, md, _ in tuples}
    summary = _ymm_summary(tuples)
    cells = {
        "id": product_id,
        "handle": handle,
        "title": title,
        "fits_on": _upper_fits_on_json_cell(fits_on),
        "fits_on_year": _upper_plain_metafield_cell(_pipe_join_sorted(years)),
        "fits_on_make": _upper_plain_metafield_cell(_pipe_join_sorted(makes)),
        "fits_on_model": _upper_plain_metafield_cell(_pipe_join_sorted(models)),
        "fits_on_year_new": "",
        "fits_on_make_new": "",
        "fits_on_model_new": "",
        "ymm_summary": _upper_plain_metafield_cell(summary),
        "parts_compartment": "",
        "parts_floor": "",
        "parts_shelve": "",
        "parts_list": "",
        "parts_box": "",
        "parts_row": "",
        "global_fits_on_make_new": "",
        "global_fits_on_model_new": "",
        "global_fits_on_year_new": "",
        "MPN": _upper_plain_metafield_cell(mpn),
    }
    return [cells[h] for h in METAFIELDS_HEADER]


def fetch_seatcovers(dest: Path) -> int:
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

    url = f"https://{shop}/admin/api/{ver}/graphql.json"
    sess = requests.Session()
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    query = """
    query($q: String!, $cursor: String) {
      products(first: 25, query: $q, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          handle
          title
          status
          vendor
          productType
          options { name values }
          featuredImage { url }
          variants(first: 30) {
            nodes {
              sku
              price
              title
              selectedOptions { name value }
            }
          }
          ymm: metafield(namespace: "global", key: "ymm_summary") { value }
          fits: metafield(namespace: "global", key: "fits_on") { value }
        }
      }
    }
    """

    def gql(variables: dict, retries: int = 8) -> dict:
        for i in range(retries):
            r = sess.post(
                url, headers=headers, json={"query": query, "variables": variables}, timeout=90
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
                raise RuntimeError(errors)
            return body["data"]
        raise RuntimeError("te vaak throttled")

    nodes: list[dict] = []
    cursor = None
    print("Seat Cover producten ophalen…", flush=True)
    while True:
        data = gql({"q": 'product_type:"Seat Cover"', "cursor": cursor})
        page = data["products"]
        nodes.extend(page["nodes"])
        print(f"  …{len(nodes)}", flush=True)
        if not page["pageInfo"]["hasNextPage"]:
            break
        cursor = page["pageInfo"]["endCursor"]
        time.sleep(0.35)

    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8") as f:
        for n in nodes:
            f.write(json.dumps(n, ensure_ascii=False) + "\n")
    return len(nodes)


def load_nodes(path: Path) -> list[dict]:
    if not path.is_file():
        raise SystemExit(f"Bronbestand ontbreekt: {path} (gebruik --refresh)")
    out: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description="Dry-run YMM uit seatcover-titels")
    ap.add_argument("--raw", type=Path, default=DEFAULT_RAW, help="JSONL cache")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--refresh", action="store_true", help="Opnieuw ophalen via GraphQL")
    ap.add_argument(
        "--include-existing",
        action="store_true",
        help="Ook producten die al ymm_summary/fits_on hebben in de import-CSV",
    )
    args = ap.parse_args()

    if args.refresh:
        n = fetch_seatcovers(args.raw)
        print(f"Opgeslagen: {args.raw} ({n} producten)")

    nodes = load_nodes(args.raw)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    meta_path = args.out_dir / f"ymm_metafields_dry_run_{stamp}.csv"
    review_path = args.out_dir / f"ymm_metafields_dry_run_review_{stamp}.csv"
    latest_meta = args.out_dir / "ymm_metafields_dry_run.csv"
    latest_review = args.out_dir / "ymm_metafields_dry_run_review.csv"

    review_fields = [
        "action",
        "parse_status",
        "id",
        "handle",
        "title",
        "make",
        "model_raw",
        "model_norm",
        "model_note",
        "year_source",
        "variant_bands",
        "years_count",
        "ymm_summary_new",
        "ymm_summary_current",
        "has_fits_current",
        "skus",
    ]

    stats = defaultdict(int)
    meta_rows: list[list[str]] = []
    review_rows: list[dict] = []

    for node in nodes:
        stats["total"] += 1
        pid = _numeric_product_id(node.get("id") or "")
        handle = (node.get("handle") or "").strip()
        title = re.sub(r"\s+", " ", (node.get("title") or "")).strip()
        current_ymm = ((node.get("ymm") or {}) or {}).get("value") or ""
        current_fits = ((node.get("fits") or {}) or {}).get("value") or ""
        has_existing = bool(current_ymm.strip() or current_fits.strip())
        skus = [
            (v.get("sku") or "").strip()
            for v in ((node.get("variants") or {}).get("nodes") or [])
            if (v.get("sku") or "").strip()
        ]
        mpn = skus[0] if skus else ""

        tuples, meta = tuples_from_product(node)
        summary_new = _ymm_summary(tuples) if tuples else ""

        if meta["parse_status"].startswith("skip"):
            stats[meta["parse_status"]] += 1
            action = meta["parse_status"]
        elif has_existing and not args.include_existing:
            stats["skip_has_existing_ymm"] += 1
            action = "skip_has_existing_ymm"
        elif not tuples:
            stats["skip_empty_tuples"] += 1
            action = "skip_empty_tuples"
        else:
            stats["would_write"] += 1
            action = "would_write"
            meta_rows.append(metafields_row(pid, handle, title, tuples, mpn=mpn))

        review_rows.append(
            {
                "action": action,
                "parse_status": meta["parse_status"],
                "id": pid,
                "handle": handle,
                "title": title,
                "make": meta.get("make") or "",
                "model_raw": meta.get("model_raw") or "",
                "model_norm": meta.get("model_norm") or "",
                "model_note": meta.get("model_note") or "",
                "year_source": meta.get("year_source") or "",
                "variant_bands": meta.get("variant_bands") or "",
                "years_count": str(len({y for _, _, y in tuples})),
                "ymm_summary_new": summary_new,
                "ymm_summary_current": current_ymm,
                "has_fits_current": "1" if current_fits.strip() else "0",
                "skus": " | ".join(skus),
            }
        )

    # Metafields Manager (komma)
    with meta_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=",", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        w.writerow(METAFIELDS_HEADER)
        w.writerows(meta_rows)
    latest_meta.write_bytes(meta_path.read_bytes())

    # Review (puntkomma, Excel NL)
    with review_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=review_fields,
            delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        w.writeheader()
        w.writerows(review_rows)
    latest_review.write_bytes(review_path.read_bytes())

    print(f"Totaal Seat Cover: {stats['total']}")
    print(f"Would write:       {stats['would_write']}")
    print(f"Skip existing YMM: {stats['skip_has_existing_ymm']}")
    print(f"Skip unparsed:     {stats['skip_title_unparsed']}")
    for k, v in sorted(stats.items()):
        if k not in {
            "total",
            "would_write",
            "skip_has_existing_ymm",
            "skip_title_unparsed",
        }:
            print(f"{k}: {v}")
    print(f"Metafields CSV: {meta_path}")
    print(f"Review CSV:     {review_path}")
    print(f"Latest copies:  {latest_meta.name} / {latest_review.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
