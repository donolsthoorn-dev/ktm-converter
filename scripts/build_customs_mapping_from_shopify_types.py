#!/usr/bin/env python3
"""
Bouw customs mapping CSV op basis van Shopify product types in Supabase mirror.

Doel:
- Per variant-SKU een HS-code en land van herkomst afleiden via type-regels.
- Output in formaat dat `build_pricelist_supabase_staging.py --customs-map-csv ...` direct leest.

Deze eerste batch gebruikt handmatig bevestigde type-regels uit de HS-sessie.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

_REQUEST_TIMEOUT = (30, 120)
_PAGE = 1000

# Bevestigde 6-cijferige defaults.
TYPE_TO_HS_CONFIRMED: dict[str, str] = {
    "Seat Cover": "871410",
    "HSQ - Seatcover Offroad": "871410",
    "Partstream": "871410",
    "Archive": "871410",
    "HSQ - Archive": "871410",
    "Spare parts": "871410",
    "Spareparts Functional": "871410",
    "Trim parts": "871410",
    "Graphics & Stickers": "491199",
    "T-shirts and Polos": "610910",
    "T-Shirts and Polos": "610910",
    "HSQ - Tees and Polos": "610910",
    "Trousers and shorts": "620343",
    "Gloves": "611693",
    "Helmets": "650610",
    "Hoodies, sweatshirts and sweat jackets": "611020",
    "HSQ - Longsleeves and Hoodies": "611020",
    "Archiv": "871410",
    "WP - Archiv": "871410",
    "Jerseys": "611030",
    "HSQ - Jerseys": "611030",
    "Bicycle Jerseys": "611030",
    "Caps and beanies": "650500",
    "Boots": "640399",
    "Shoes and socks": "640399",
    "E TrOnroad": "871200",
    "HSQ - Trim parts": "871410",
    "Piston kit": "871410",
    "WP - Spare Parts": "871410",
    "Protectors": "871410",
    "HSQ - Pants": "620343",
    "Bicycle Pants": "620343",
    "HSQ - Gloves": "611693",
    "HSQ - Helmets": "650610",
    "Bicycle Shoes": "640399",
    "Seats": "871410",
    "Engine Protection": "871410",
    "Plastic kits": "871410",
    "Drivetrain kit": "871410",
    "HSQ - Piston kit": "871410",
    "E MTB Fully": "871200",
    "Gravel": "871200",
    "Tool/transport": "820559",
    "Event Material": "901720",
    "Casual & Accessories": "871410",
    "Accessoires": "871410",
    "Lifestyle": "871410",
    "Shirts": "610510",
    "WP - Jackets": "620193",
    "Longsleeves and Hoodies": "611020",
    "HSQ - Best Deal": "871410",
    "Hangers": "732690",
    "WP - New": "871410",
    "WP - Transportation": "420292",
}

# Hoofdstuk bevestigd, maar nog geen 6-cijferige keuze.
TYPE_CHAPTER_ONLY: dict[str, str] = {
    "Jackets": "61",
    "HSQ - Jackets": "61",
}

# Motorcycles/bikes: hoofdstuk 8711 subheading op basis van cilinderinhoud (cc) in titel.
TYPE_8711_BY_CC: set[str] = {
    "WP - Bikes",
    "HSQ - Bikes",
    "Motorcycles",
    "HSQ - Motorcycles",
    "Street",
    "Offroad",
}
HS_8711_FALLBACK_WHEN_NO_CC = "871120"


def _normalize_type_key(raw: str) -> str:
    s = (raw or "").replace("\u00a0", " ").strip()
    return " ".join(s.split())


TYPE_TO_HS_CONFIRMED_NORM = {
    _normalize_type_key(k): v for k, v in TYPE_TO_HS_CONFIRMED.items()
}
TYPE_CHAPTER_ONLY_NORM = {_normalize_type_key(k): v for k, v in TYPE_CHAPTER_ONLY.items()}
TYPE_8711_BY_CC_NORM = {_normalize_type_key(k) for k in TYPE_8711_BY_CC}


def _hs_for_jacket_title(title: str) -> str:
    t = (title or "").strip().lower()
    women_keys = ("women", "woman", "girls", "girl", "ladies", "female")
    men_keys = ("men", "man", "boys", "boy", "male")
    if any(k in t for k in women_keys):
        return "610230"
    if any(k in t for k in men_keys):
        return "610130"
    # Fallback bij onduidelijke titel.
    return "610130"


def _extract_cc_from_title(title: str) -> int | None:
    t = (title or "").strip().lower()
    if not t:
        return None
    # Veel voorkomende notaties: 125, 390, 690, 1290, "250 cc", "450cc"
    import re

    for m in re.finditer(r"(?<!\d)(\d{2,4})(?:\s*cc)?(?!\d)", t):
        n = int(m.group(1))
        if 50 <= n <= 3000:
            return n
    return None


def _hs_8711_from_cc(cc: int | None) -> str | None:
    if cc is None:
        return None
    if cc <= 50:
        return "871110"
    if cc <= 250:
        return "871120"
    if cc <= 500:
        return "871130"
    if cc <= 800:
        return "871140"
    return "871150"


def _rest_base() -> str:
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    if not url:
        raise SystemExit("SUPABASE_URL ontbreekt")
    return f"{url}/rest/v1"


def _headers() -> dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        raise SystemExit("SUPABASE_SERVICE_ROLE_KEY ontbreekt")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }


def _fetch_paginated(
    sess: requests.Session,
    base: str,
    headers: dict[str, str],
    table: str,
    select: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        r = sess.get(
            f"{base}/{table}",
            headers=headers,
            params={
                "select": select,
                "limit": str(_PAGE),
                "offset": str(offset),
            },
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        rows = r.json() or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < _PAGE:
            break
        offset += _PAGE
    return out


def _load_type_suggestions(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            t = _normalize_type_key(str(row.get("type") or ""))
            hs = str(row.get("suggested_hs") or "").strip()
            if t and hs:
                out[t] = hs
    return out


def _infer_hs_from_type_keywords(type_norm: str) -> str | None:
    t = (type_norm or "").lower()
    # Bags / luggage
    if "luggage" in t or "bag" in t:
        return "420292"
    # Headwear / caps
    if "headwear" in t or "beanie" in t or "cap" in t:
        return "650500"
    # Gloves
    if "glove" in t:
        return "611693"
    # Stickers / decals
    if "sticker" in t or "decal" in t:
        return "491199"
    # Oils / fluids
    if "oil" in t:
        return "271019"
    # Measuring / gauges / meters
    if "measuring" in t or "meter" in t or "gauge" in t:
        return "901720"
    # Generic tools
    if (
        "tool" in t
        or "wrench" in t
        or "pliers" in t
        or "pressing" in t
        or "timing" in t
        or "spark plug" in t
        or "air pump" in t
    ):
        return "820559"
    # Wheels/parts
    if "wheel" in t or "rim" in t or "spoke" in t:
        return "871410"
    # Lighting/electrics style
    if "flash" in t:
        return "851220"
    # Chassis/protection/spare parts style
    if (
        "spare" in t
        or "protection" in t
        or "chassis" in t
        or "fork" in t
        or "shock" in t
        or "triple clamp" in t
        or "brake pad" in t
        or "cylinder" in t
        or "piston" in t
        or "shim" in t
        or "valve" in t
        or "hand protection" in t
        or "tank protection" in t
        or "bar mount" in t
        or "plastic parts" in t
        or "exhaust" in t
    ):
        return "871410"
    # Default for remaining misc merch/meta-like types.
    if t in ("images", "mannequin", "new", "other", "other accessoires", "racetrack and camping"):
        return "871410"
    return None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--output-csv",
        default=str(ROOT / "input" / "customs_mapping_types_batch1.csv"),
        help="Output mapping CSV (sku;hs_code;country_of_origin;source)",
    )
    p.add_argument(
        "--chapter-review-csv",
        default=str(ROOT / "output" / "customs_type_chapter_review.csv"),
        help="Rapport met types die alleen hoofdstuk-keuze hebben (nog geen 6-cijferige HS)",
    )
    p.add_argument(
        "--country",
        default="AT",
        help="Vaste country_of_origin voor deze batch (default: AT)",
    )
    p.add_argument(
        "--type-suggestions-csv",
        default=str(ROOT / "output" / "type_hs_suggestions.csv"),
        help="CSV met dominante HS per type (fallback voor resterende types)",
    )
    args = p.parse_args()

    base = _rest_base()
    headers = _headers()
    sess = requests.Session()
    sess.trust_env = False
    suggestion_map = _load_type_suggestions(Path(args.type_suggestions_csv))

    print("Supabase: shopify_products laden…", flush=True)
    products = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_products",
        "shopify_product_id,type",
    )
    product_type_by_id: dict[int, str] = {}
    for row in products:
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        t = str(row.get("type") or "").strip()
        if t:
            product_type_by_id[int(pid)] = t

    print("Supabase: shopify_variants laden…", flush=True)
    variants = _fetch_paginated(
        sess,
        base,
        headers,
        "shopify_variants",
        "shopify_variant_id,shopify_product_id,sku,title",
    )

    out_rows: dict[str, dict[str, str]] = {}
    chapter_review: dict[str, set[str]] = defaultdict(set)
    fixed_country = (args.country or "").strip().upper()

    for row in variants:
        sku = str(row.get("sku") or "").strip().upper()
        if not sku:
            continue
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        t_raw = product_type_by_id.get(int(pid), "")
        if not t_raw:
            continue
        t = _normalize_type_key(t_raw)
        title = str(row.get("title") or "")
        hs = TYPE_TO_HS_CONFIRMED_NORM.get(t)
        if hs:
            out_rows[sku] = {
                "sku": sku,
                "hs_code": hs,
                "country_of_origin": fixed_country,
                "source": f"type_rule:{t_raw}",
            }
            continue
        if t in TYPE_8711_BY_CC_NORM:
            cc = _extract_cc_from_title(title)
            hs_8711 = _hs_8711_from_cc(cc)
            if hs_8711:
                out_rows[sku] = {
                    "sku": sku,
                    "hs_code": hs_8711,
                    "country_of_origin": fixed_country,
                    "source": f"type_rule:{t_raw}:cc_{cc}",
                }
            else:
                out_rows[sku] = {
                    "sku": sku,
                    "hs_code": HS_8711_FALLBACK_WHEN_NO_CC,
                    "country_of_origin": fixed_country,
                    "source": f"type_rule:{t_raw}:fallback_no_cc",
                }
            continue
        chapter = TYPE_CHAPTER_ONLY_NORM.get(t)
        if chapter:
            if chapter == "61" and t in ("Jackets", "HSQ - Jackets"):
                hs_jacket = _hs_for_jacket_title(title)
                out_rows[sku] = {
                    "sku": sku,
                    "hs_code": hs_jacket,
                    "country_of_origin": fixed_country,
                    "source": f"type_rule:{t_raw}:gender_split",
                }
            else:
                chapter_review[t_raw].add(sku)
            continue

        # Pre-approved fallback voor saddle/seat-specifieke types (behalve seat covers).
        if ("saddle" in t or "seat" in t) and "seat cover" not in t and "seatcover" not in t:
            out_rows[sku] = {
                "sku": sku,
                "hs_code": "871495",
                "country_of_origin": fixed_country,
                "source": f"type_rule:{t_raw}:auto_saddle_seat",
            }
            continue

        # Beleid: ook bij lage coverage een default per type gebruiken (dominante HS uit mirror).
        hs_dom = suggestion_map.get(t)
        if hs_dom:
            out_rows[sku] = {
                "sku": sku,
                "hs_code": hs_dom,
                "country_of_origin": fixed_country,
                "source": f"type_rule:{t_raw}:dominant_hs",
            }
            continue

        hs_kw = _infer_hs_from_type_keywords(t)
        if hs_kw:
            out_rows[sku] = {
                "sku": sku,
                "hs_code": hs_kw,
                "country_of_origin": fixed_country,
                "source": f"type_rule:{t_raw}:keyword_rule",
            }
            continue

        # Geen regel gevonden -> review.
        chapter_review[t_raw].add(sku)

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["sku", "hs_code", "country_of_origin", "source"],
            delimiter=";",
        )
        writer.writeheader()
        for sku in sorted(out_rows):
            writer.writerow(out_rows[sku])

    review_path = Path(args.chapter_review_csv)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with open(review_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(["type", "chapter", "sku_count"])
        for t in sorted(chapter_review):
            nt = _normalize_type_key(t)
            chapter = TYPE_CHAPTER_ONLY_NORM.get(
                nt, "8711" if nt in TYPE_8711_BY_CC_NORM else ""
            )
            writer.writerow([t, chapter, len(chapter_review[t])])

    print(f"Mapping geschreven: {output_path} ({len(out_rows)} SKU's)", flush=True)
    if chapter_review:
        pending = sum(len(skus) for skus in chapter_review.values())
        print(
            f"Nog te verfijnen (alleen hoofdstuk): {pending} SKU's over {len(chapter_review)} type(s). "
            f"Zie: {review_path}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
