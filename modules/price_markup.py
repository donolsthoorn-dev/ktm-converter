"""
Vaste verkoopprijs-markup (9%) op geselecteerde Shopify product types.

Toegepast op de CSV-prijs (incl. btw) in de prijs/ETA-pipeline, zodat latere
prijslijst-updates niet terugvallen op de kale ERP-prijs.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

MARKUP_FACTOR = Decimal("1.09")
MARKUP_PERCENT = 9
CENT = Decimal("0.01")

# Types uit Excel (tabblad Products) die géén markup krijgen.
EXCLUDED_TYPES = frozenset(
    {
        "WP - Cartridge",
        "WP - Gloves",
        "WP - Lowering kit",
        "WP - Preload adjuster",
        "WP - Protection",
    }
)

# Alle overige types uit dat tabblad, inclusief types die nu 0 shop-hits hebben
# (nieuwe/terugkerende producten van hetzelfde type krijgen de markup wél).
MARKUP_TYPES = frozenset(
    {
        "Additional",
        "Bleeder tool",
        "Crankshaft pressing tool",
        "Diagnosetool",
        "Electrical system / diagnosis",
        "Engine fixing arm",
        "Event Material",
        "Extractor tool",
        "Folder",
        "Fork",
        "Groove nut wrench",
        "Hangers",
        "HSQ - Additional",
        "HSQ - Bleeder tool",
        "HSQ - Crankshaft pressing tool",
        "HSQ - Diagnosetool",
        "HSQ - Electrical system / diagnosis",
        "HSQ - Engine fixing arm",
        "HSQ - Event Material",
        "HSQ - Extractor tool",
        "HSQ - Groove nut wrench",
        "HSQ - Hangers",
        "HSQ - Images",
        "HSQ - Lift and work stand",
        "HSQ - Mannequin",
        "HSQ - Measuring tool & setting gauge",
        "HSQ - Merchandising Material",
        "HSQ - Other tools",
        "HSQ - Panels & Accessories",
        "HSQ - Piston tool",
        "HSQ - Pliers",
        "HSQ - POS",
        "HSQ - Pressing tool",
        "HSQ - Protecting sleeves & protection caps",
        "HSQ - Shock absorber",
        "HSQ - Spark plug wrench",
        "HSQ - Support tool",
        "HSQ - Transmission tool",
        "HSQ - Valve and timing tool",
        "HV tool",
        "Images",
        "Mannequin",
        "Measuring tool & setting gauge",
        "Merchandising Material",
        "Other tools",
        "Panels & Accessories",
        "Piston tool",
        "Pliers",
        "Pressing tool",
        "Protecting sleeves & protection caps",
        "Shock absorber",
        "Support tool",
        "Transmission tool",
        "Valve and timing tool",
        "WP - Adaptor",
        "WP - Adjuster tool",
        "WP - Air pump",
        "WP - Archiv",
        "WP - Bikes",
        "WP - Clamping stand",
        "WP - Drift",
        "WP - Event material",
        "WP - Filling tool",
        "WP - Flow meter",
        "WP - Folder",
        "WP - Fork oil",
        "WP - Grease",
        "WP - Measuring tool",
        "WP - Merchandising material",
        "WP - Mounting tool",
        "WP - Other Accessoires",
        "WP - Poster",
        "WP - Reducing ring",
        "WP - Sale",
        "WP - Shock absorber oil",
        "WP - Socket",
        "WP - Spare Parts",
        "WP - Special parts",
        "WP - Support tool",
        "WP - Toolboard",
        "WP - Tools",
        "WP - Vacuum pump",
        "WP - Wrench",
    }
)

_MARKUP_TYPES_FOLD = frozenset(t.casefold() for t in MARKUP_TYPES)


def product_type_has_markup(product_type: str | None) -> bool:
    t = (product_type or "").strip()
    if not t:
        return False
    return t.casefold() in _MARKUP_TYPES_FOLD


def _to_decimal(raw: Any) -> Decimal | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, Decimal):
        return raw
    try:
        return Decimal(str(raw).strip().replace(",", "."))
    except Exception:
        return None


def apply_price_markup_decimal(
    price: Decimal | None, product_type: str | None
) -> Decimal | None:
    if price is None:
        return None
    if not product_type_has_markup(product_type):
        return price
    return (price * MARKUP_FACTOR).quantize(CENT, rounding=ROUND_HALF_UP)


def apply_price_markup_str(price: str | None, product_type: str | None) -> str | None:
    if price is None or str(price).strip() == "":
        return price
    d = _to_decimal(price)
    if d is None:
        return price
    out = apply_price_markup_decimal(d, product_type)
    if out is None:
        return price
    return f"{out:.2f}"


def apply_price_markup_float(price: float | None, product_type: str | None) -> float | None:
    if price is None:
        return None
    d = apply_price_markup_decimal(Decimal(str(price)), product_type)
    if d is None:
        return price
    return float(d)


def product_type_from_mirror_row(row: dict[str, Any] | None) -> str:
    if not row:
        return ""
    t = str(row.get("type") or "").strip()
    if t:
        return t
    raw = row.get("raw")
    if isinstance(raw, dict):
        return str(raw.get("productType") or "").strip()
    return ""


def fetch_product_types_by_id() -> dict[int, str]:
    """
    Shopify product_id → product type uit Supabase-spiegel.
    Leeg dict als URL/key ontbreekt (dan geen markup in die run).
    """
    import os

    import requests

    from modules.env_loader import load_project_env

    load_project_env()

    url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        return {}
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    out: dict[int, str] = {}
    page = 1000
    offset = 0
    sess = requests.Session()
    sess.trust_env = False
    select = "shopify_product_id,type"
    while True:
        r = sess.get(
            f"{url}/rest/v1/shopify_products",
            headers=headers,
            params={
                "select": select,
                "limit": str(page),
                "offset": str(offset),
                "order": "shopify_product_id.asc",
            },
            timeout=(30, 120),
        )
        if r.status_code == 400 and "type" in select:
            select = "shopify_product_id,raw"
            offset = 0
            out.clear()
            continue
        r.raise_for_status()
        chunk = r.json() or []
        if not chunk:
            break
        for row in chunk:
            pid = row.get("shopify_product_id")
            if pid is None:
                continue
            ptype = product_type_from_mirror_row(row)
            if ptype:
                out[int(pid)] = ptype
        if len(chunk) < page:
            break
        offset += page
    return out
