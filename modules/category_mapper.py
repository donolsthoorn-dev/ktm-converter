"""
Shopify standaard-Category (taxonomie) afleiden uit Type, tags, titel en body.

Prioriteit (hoog → laag):
  1. Exacte / prefix product Type-mapping
  2. Bekende tags (PowerWear, PowerParts, …)
  3. Zoekwoorden in titel + omschrijving
  4. Default: Motor Vehicle Parts

Zie canvas `shopify-category-mapping` voor het overzicht.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_CATEGORY = "Motor Vehicle Parts"

# Shopify admin "Category" = standard product taxonomy (full breadcrumb with " > ").
# See https://shopify.github.io/product-taxonomy/
DEFAULT_SHOPIFY_PRODUCT_CATEGORY = (
    "Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts"
)

CLOTHING = "Apparel & Accessories > Clothing"
VEHICLE_PARTS = DEFAULT_SHOPIFY_PRODUCT_CATEGORY
TOOLS = "Hardware > Tools"
BAGS = "Luggage & Bags > Backpacks"
SPORT_BAGS = BAGS
HELMETS = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorcycle Protective Gear > Motorcycle Helmets"
)
GLOVES = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorcycle Protective Gear > Motorcycle Gloves"
)
# Geen aparte "Motorcycle Boots" in huidige Shopify-taxonomie → schoenen.
BOOTS = "Apparel & Accessories > Shoes"
GOGGLES = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorcycle Protective Gear > Motorcycle Goggles"
)
BIKES_E = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycles > Electric Bikes"
)
BIKE_PARTS = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Parts & Accessories"
)
BIKE_CLOTHING = "Apparel & Accessories > Clothing"
GIFTCARD = "Arts & Entertainment > Party & Celebration > Gift Giving > Gift Cards"
# Geen betrouwbare "Motor Vehicle Fluids"-node → parent parts.
OILS = VEHICLE_PARTS

# Values aligned with Shopify's English taxonomy (same keys as map_category() outcomes).
_SHOPIFY_PRODUCT_CATEGORY_BY_GOOGLE = {
    "Motor Vehicle Parts": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    "Tools": TOOLS,
    "Clothing": CLOTHING,
    "Bicycles": BIKES_E,
}

# KTM XML grandparent / legacy category → Google-style bucket (export CSV).
CATEGORY_MAP = {
    "PowerParts": "Motor Vehicle Parts",
    "Functional": "Motor Vehicle Parts",
    "Handlebars/instruments/electrics": "Motor Vehicle Parts",
    "Exhaust systems": "Motor Vehicle Parts",
    "Protection": "Motor Vehicle Parts",
    "Seats": "Motor Vehicle Parts",
    "Chassis/triple clamp": "Motor Vehicle Parts",
    "Engine": "Motor Vehicle Parts",
    "Carbon": "Motor Vehicle Parts",
    "Wheels": "Motor Vehicle Parts",
    "Brakes": "Motor Vehicle Parts",
    "Cooling": "Motor Vehicle Parts",
    "Chains/Sprockets": "Motor Vehicle Parts",
    "Suspension": "Motor Vehicle Parts",
    "Original Spare Part Kits": "Motor Vehicle Parts",
    "Trim parts/decals": "Motor Vehicle Parts",
    "Special tools": "Tools",
    "Tool/transport": "Tools",
    "Casual and Accessories": "Clothing",
    "PowerWear": "Clothing",
    "Electric Balance Bikes": "Bicycles",
}

# Exact Shopify product_type → taxonomy path (case-insensitive match on stripped type).
TYPE_EXACT: dict[str, str] = {
    # Apparel / PowerWear types
    "t-shirts and polos": CLOTHING,
    "hoodies, sweatshirts and sweat jackets": CLOTHING,
    "jackets": CLOTHING,
    "trousers and shorts": CLOTHING,
    "functional underwear": CLOTHING,
    "shoes and socks": BOOTS,
    "caps and beanies": CLOTHING,
    "casual": CLOTHING,
    "casual and accessories": CLOTHING,
    "powerwear": CLOTHING,
    "gloves": GLOVES,
    "helmets": HELMETS,
    "goggles": GOGGLES,
    "accessoires": CLOTHING,
    # Parts families
    "powerparts": VEHICLE_PARTS,
    "partstream": VEHICLE_PARTS,
    "batteries lithium": VEHICLE_PARTS,
    "functional": VEHICLE_PARTS,
    "engine": VEHICLE_PARTS,
    "brakes": VEHICLE_PARTS,
    "cooling": VEHICLE_PARTS,
    "suspension": VEHICLE_PARTS,
    "wheels": VEHICLE_PARTS,
    "seats": VEHICLE_PARTS,
    "protection": VEHICLE_PARTS,
    "carbon": VEHICLE_PARTS,
    "exhaust systems": VEHICLE_PARTS,
    "chains/sprockets": VEHICLE_PARTS,
    "trim parts/decals": VEHICLE_PARTS,
    "original spare part kits": VEHICLE_PARTS,
    "handlebars/instruments/electrics": VEHICLE_PARTS,
    "air filter box": VEHICLE_PARTS,
    "air filter cover": VEHICLE_PARTS,
    "air filter pre-oiled ktm": VEHICLE_PARTS,
    "air filter standaard ktm": VEHICLE_PARTS,
    "alarm system": VEHICLE_PARTS,
    "additional": VEHICLE_PARTS,
    "2-stroke offroad": VEHICLE_PARTS,
    "4-stroke offroad": VEHICLE_PARTS,
    # Tools / luggage
    "special tools": TOOLS,
    "tool/transport": TOOLS,
    "backpacks / bags": SPORT_BAGS,
    "bags and luggage": SPORT_BAGS,
    # Bikes / balance
    "electric balance bikes": BIKES_E,
    "bicycle": BIKE_PARTS,
    # Other
    "gift card": GIFTCARD,
    "gift cards": GIFTCARD,
    "archive": VEHICLE_PARTS,
    "archiv": VEHICLE_PARTS,
}

# Prefix rules: product_type.startswith(prefix) → category (checked after exact).
TYPE_PREFIX: list[tuple[str, str]] = [
    ("bicycle first layer", BIKE_CLOTHING),
    ("bicycle gloves", BIKE_CLOTHING),
    ("bicycle jackets", BIKE_CLOTHING),
    ("bicycle jerseys", BIKE_CLOTHING),
    ("bicycle pants", BIKE_CLOTHING),
    ("bicycle shorts", BIKE_CLOTHING),
    ("bicycle socks", BIKE_CLOTHING),
    ("bicycle shoes", BIKE_CLOTHING),
    ("bicycle helmets", HELMETS),
    ("bicycle heads and scarfs", CLOTHING),
    ("bicycle sunglasses", GOGGLES),
    ("bicycle backpacks", SPORT_BAGS),
    ("bicycle bags", SPORT_BAGS),
    ("bicycle ", BIKE_PARTS),
    ("hsq -", VEHICLE_PARTS),
    ("wp ", VEHICLE_PARTS),
]

# Tag → category (first matching tag wins; order = priority).
TAG_RULES: list[tuple[str, str]] = [
    ("powerwear", CLOTHING),
    ("casual", CLOTHING),
    ("electric balance bikes", BIKES_E),
    ("special tools", TOOLS),
    ("tool/transport", TOOLS),
    ("powerparts", VEHICLE_PARTS),
    ("partstream", VEHICLE_PARTS),
    ("functional", VEHICLE_PARTS),
]

# (label, compiled regex on title+body lowercase, category) — first match wins.
_TEXT_PATTERNS: list[tuple[str, re.Pattern[str], str]] = [
    ("text:gift card", re.compile(r"\bgift\s*card\b|cadeaubon"), GIFTCARD),
    ("text:helmet", re.compile(r"\bhelmets?\b|\bhelm\b"), HELMETS),
    ("text:gloves", re.compile(r"\bgloves?\b|\bhandschoen"), GLOVES),
    # Alleen echte schoenen/boots — niet "boot" in technische zinnen.
    ("text:boots", re.compile(r"\b(mx\s+boots?|motocross\s+boots?|riding\s+boots?|laarzen)\b"), BOOTS),
    ("text:goggles", re.compile(r"\bgoggles?\b"), GOGGLES),
    (
        "text:apparel",
        re.compile(
            r"\b(t-?shirts?\b|hoodie|sweatshirt|sweat\s*jacket|jersey|"
            r"beanie|polo\b|crewneck)\b"
        ),
        CLOTHING,
    ),
    ("text:tools", re.compile(r"\b(special tool|torque wrench|tool kit|gereedschap)\b"), TOOLS),
    # Motorex/olie → parts (geen aparte fluids-node); geen losse "olie" in NL-HTML.
    ("text:oil", re.compile(r"\b(engine oil|fork oil|brake fluid|motorex)\b"), OILS),
    (
        "text:bike",
        re.compile(r"\b(electric balance|e-?bike|bicycle|fiets)\b"),
        BIKES_E,
    ),
    (
        "text:parts",
        re.compile(
            r"\b(filter|brake|clutch|sprocket|chain|piston|gasket|bearing|"
            r"exhaust|radiator|fork|shock|bolt|nut|washer|oil seal|o-?ring)\b"
        ),
        VEHICLE_PARTS,
    ),
]


@dataclass(frozen=True)
class CategoryDecision:
    """Resultaat van resolve_shopify_product_category."""

    path: str
    source: str  # type:… | tag:… | text:… | xml:… | default
    bucket: str  # korte label voor rapporten


def map_category(ktm_category: str | None) -> str:
    if not ktm_category:
        return DEFAULT_CATEGORY
    return CATEGORY_MAP.get(ktm_category.strip(), DEFAULT_CATEGORY)


def map_shopify_product_category(ktm_category: str | None) -> str:
    """Full Shopify standard category path for CSV column Product category."""
    google = map_category(ktm_category)
    return _SHOPIFY_PRODUCT_CATEGORY_BY_GOOGLE.get(google, DEFAULT_SHOPIFY_PRODUCT_CATEGORY)


def _bucket_for_path(path: str) -> str:
    if path == CLOTHING or path == BIKE_CLOTHING:
        return "Clothing"
    if path == TOOLS:
        return "Tools"
    if path in (BIKES_E, BIKE_PARTS):
        return "Bicycles / bike parts"
    if path == HELMETS:
        return "Helmets"
    if path == GLOVES:
        return "Gloves"
    if path == BOOTS:
        return "Boots"
    if path == GOGGLES:
        return "Goggles"
    if path == SPORT_BAGS or path == BAGS:
        return "Bags"
    if path == GIFTCARD:
        return "Gift cards"
    # OILS deelt pad met VEHICLE_PARTS → niet apart labelen
    return "Motor Vehicle Parts"


def resolve_shopify_product_category(
    *,
    product_type: str | None = None,
    tags: str | list[str] | None = None,
    title: str | None = None,
    body_html: str | None = None,
    xml_category: str | None = None,
) -> CategoryDecision:
    """
    Bepaal Shopify Category-pad + bron.

    Volgorde: Type → tags → titel/body → XML-categorie → default.
    """
    ptype = (product_type or "").strip()
    if ptype:
        key = ptype.lower()
        if key in TYPE_EXACT:
            path = TYPE_EXACT[key]
            return CategoryDecision(path, f"type:{ptype}", _bucket_for_path(path))
        for prefix, path in TYPE_PREFIX:
            if key.startswith(prefix):
                return CategoryDecision(
                    path, f"type-prefix:{prefix.strip()}", _bucket_for_path(path)
                )

    tag_list: list[str]
    if tags is None:
        tag_list = []
    elif isinstance(tags, str):
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    else:
        tag_list = [str(t).strip() for t in tags if str(t).strip()]

    tag_lower = {t.lower() for t in tag_list}
    for tag_key, path in TAG_RULES:
        if tag_key in tag_lower:
            return CategoryDecision(path, f"tag:{tag_key}", _bucket_for_path(path))

    blob = f"{title or ''}\n{body_html or ''}".lower()
    # Strip simple HTML tags for keyword scan
    blob = re.sub(r"<[^>]+>", " ", blob)
    for label, pattern, path in _TEXT_PATTERNS:
        if pattern.search(blob):
            return CategoryDecision(path, label, _bucket_for_path(path))

    if xml_category and xml_category.strip():
        path = map_shopify_product_category(xml_category)
        return CategoryDecision(path, f"xml:{xml_category.strip()}", _bucket_for_path(path))

    return CategoryDecision(
        DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
        "default",
        _bucket_for_path(DEFAULT_SHOPIFY_PRODUCT_CATEGORY),
    )
