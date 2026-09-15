"""
Shopify standaard-Category (taxonomie) afleiden uit Type, tags, titel en body.

Prioriteit (hoog → laag):
  1. Specifiek product Type (exact of prefix; HSQ-/WP-prefix genegeerd)
  2. Zoekwoorden in titel + omschrijving → specifieke taxonomie-tak
  3. Specifieke tags (PowerWear, Casual, …)
  4. Default: Motor Vehicle Parts (alleen als niets matcht)

Generieke types (Partstream, Archive, PowerParts, Lifestyle, …) forceren
géén type-hit; die gaan door naar titel/keywords.

Zie canvas `shopify-category-mapping` voor het overzicht.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_CATEGORY = "Motor Vehicle Parts"

# Shopify admin "Category" = standard product taxonomy (full breadcrumb with " > ").
# See https://shopify.github.io/product-taxonomy/
_MVP = "Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts"
DEFAULT_SHOPIFY_PRODUCT_CATEGORY = _MVP

# --- Motor Vehicle Parts children (specifiek) ---
BRAKING = f"{_MVP} > Motor Vehicle Braking"
UPHOLSTERY = f"{_MVP} > Motor Vehicle Carpet & Upholstery"
CLIMATE = f"{_MVP} > Motor Vehicle Climate Control"
CONTROLS = f"{_MVP} > Motor Vehicle Controls"
OIL_CIRC = f"{_MVP} > Motor Vehicle Engine Oil Circulation"
ENGINE_PARTS = f"{_MVP} > Motor Vehicle Engine Parts"
ENGINES = f"{_MVP} > Motor Vehicle Engines"
EXHAUST = f"{_MVP} > Motor Vehicle Exhaust"
FRAME_BODY = f"{_MVP} > Motor Vehicle Frame & Body Parts"
FUEL = f"{_MVP} > Motor Vehicle Fuel Systems"
INTERIOR = f"{_MVP} > Motor Vehicle Interior Fittings"
LIGHTING = f"{_MVP} > Motor Vehicle Lighting"
MIRRORS = f"{_MVP} > Motor Vehicle Mirrors"
ELECTRICAL = f"{_MVP} > Motor Vehicle Power & Electrical Systems"
SEATING = f"{_MVP} > Motor Vehicle Seating"
SENSORS = f"{_MVP} > Motor Vehicle Sensors & Gauges"
SUSPENSION = f"{_MVP} > Motor Vehicle Suspension Parts"
TOWING = f"{_MVP} > Motor Vehicle Towing"
DRIVETRAIN = f"{_MVP} > Motor Vehicle Transmission & Drivetrain Parts"
WHEELS = f"{_MVP} > Motor Vehicle Wheel Systems"
WINDOW = f"{_MVP} > Motor Vehicle Window Parts & Accessories"
COOLING = f"{_MVP} > Motor Vehicle Cooling Systems"
AIR_INTAKE = f"{_MVP} > Motor Vehicle Air Intake"

VEHICLE_PARTS = DEFAULT_SHOPIFY_PRODUCT_CATEGORY
VEHICLE_WINDOW_PARTS = WINDOW

CLOTHING = "Apparel & Accessories > Clothing"
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
BOOTS = "Apparel & Accessories > Shoes > Boots"
SHOES = "Apparel & Accessories > Shoes"
GOGGLES = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorcycle Protective Gear > Motorcycle Goggles"
)
SUNGLASSES = "Apparel & Accessories > Clothing Accessories > Sunglasses"
BIKES_E = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycles > Electric Bikes"
)
BIKES = "Sporting Goods > Outdoor Recreation > Cycling > Bicycles"
BIKE_PARTS = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Parts & Accessories"
)
BIKE_HELMETS = (
    "Sporting Goods > Outdoor Recreation > Cycling > "
    "Cycling Apparel & Accessories > Bicycle Helmets"
)
BIKE_DISPLAYS = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Accessories > "
    "Bicycle Computer Accessories > Bicycle Computer Displays"
)
BIKE_CLOTHING = CLOTHING
PHONE_CASES = (
    "Electronics > Communications > Telephony > "
    "Mobile & Smart Phone Accessories > Mobile Phone Cases"
)
GIFTCARD = "Arts & Entertainment > Party & Celebration > Gift Giving > Gift Cards"
OILS = OIL_CIRC  # dichtstbijzijnde taxonomie voor olie/vloeistoffen
DECALS = "Toys & Games > Toys > Art & Drawing Toys > Stickers & Sticker Machines"
# Event / merchandising (geen motoronderdelen).
EVENT_MATERIAL = (
    "Business & Industrial > Advertising & Marketing > Trade Show Displays"
)
FLAG_HARDWARE = (
    "Home & Garden > Lawn & Garden > Outdoor Living > Outdoor Structures > "
    "Flags & Windsocks > Flag & Windsock Accessories > "
    "Flag & Windsock Pole Mounting Hardware & Kits"
)

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

# Types die te generiek zijn → géén type-hit; laat tekst/keywords beslissen.
GENERIC_TYPES: set[str] = {
    "partstream",
    "powerparts",
    "spareparts",
    "spare parts",
    "spareparts functional",
    "archive",
    "archiv",
    "hsq - archive",
    "wp - archiv",
    "wp - spare parts",
    "wp - sale",
    "sale",
    "additional",
    "images",
    "hsq - images",
    "functional",
    "offroad",
    "street",
    "gravel",
    "road",
    "lifestyle",
    "fan gear",
    "pos",
    "software enhancements",
    "motorcycles",
}

# Exact Shopify product_type → taxonomy path (case-insensitive).
# HSQ -/WP - prefix wordt in resolve afgestript; keys hier zonder merkprefix
# (dubbele hsq-/wp- keys mogen blijven voor backwards compat).
TYPE_EXACT: dict[str, str] = {
    # Apparel / PowerWear
    "t-shirts and polos": CLOTHING,
    "tees and polos": CLOTHING,
    "tee and polos": CLOTHING,
    "longsleeves and hoodies": CLOTHING,
    "longsleeve": CLOTHING,
    "longsleeves": CLOTHING,
    "hoodies, sweatshirts and sweat jackets": CLOTHING,
    "jackets": CLOTHING,
    "trousers and shorts": CLOTHING,
    "pants and shorts": CLOTHING,
    "pants": CLOTHING,
    "functional underwear": CLOTHING,
    "shoes and socks": SHOES,
    "boots": BOOTS,
    "caps and beanies": CLOTHING,
    "headwear": CLOTHING,
    "clothing": CLOTHING,
    "casual": CLOTHING,
    "casual and accessories": CLOTHING,
    "casual & accessories": CLOTHING,
    "powerwear": CLOTHING,
    "gloves": GLOVES,
    "helmets": HELMETS,
    "hsq - helmets": HELMETS,
    "bicycle helmets": BIKE_HELMETS,
    "bicycle helmet": BIKE_HELMETS,
    "goggles": GOGGLES,
    "accessoires": CLOTHING,
    "other accessoires": EVENT_MATERIAL,
    "other accessories": EVENT_MATERIAL,
    "racetrack and camping": EVENT_MATERIAL,
    "jerseys": CLOTHING,
    "shirts": CLOTHING,
    "backpacks": BAGS,
    # Event / merchandising — alleen als tekst geen specifiekere hit heeft
    # (zie TEXT_FIRST_TYPES in resolve).
    "event material": EVENT_MATERIAL,
    "hsq - event material": EVENT_MATERIAL,
    "wp - event material": EVENT_MATERIAL,
    "merchandising material": EVENT_MATERIAL,
    "wp - merchandising material": EVENT_MATERIAL,
    "hsq - merchandising material": EVENT_MATERIAL,
    "merchandise": EVENT_MATERIAL,
    # Seating / covers
    "seat cover": SEATING,
    "seats": SEATING,
    "seats offroad": SEATING,
    "seats street": SEATING,
    "hsq - seatcover offroad": SEATING,
    # Engine
    "engine": ENGINE_PARTS,
    "engine parts, 4-stroke": ENGINE_PARTS,
    "engine parts, 2-stroke": ENGINE_PARTS,
    "engine protection": FRAME_BODY,
    "engine sale": ENGINE_PARTS,
    "piston kit": ENGINE_PARTS,
    "hsq - piston kit": ENGINE_PARTS,
    "engine fixing arm": ENGINE_PARTS,
    # Brakes / controls / drivetrain
    "brakes": BRAKING,
    "brake discs": BRAKING,
    "clutch / brake lever": CONTROLS,
    "levers": CONTROLS,
    # Cooling / intake / fuel / oil
    "cooling": COOLING,
    "air filter box": AIR_INTAKE,
    "consumables / liquids": OIL_CIRC,
    "oil products": OIL_CIRC,
    "batteries lithium": ELECTRICAL,
    # Suspension / chassis
    "suspension": SUSPENSION,
    "suspension component": SUSPENSION,
    "suspension components": SUSPENSION,
    "steering damper": SUSPENSION,
    "triple clamps": SUSPENSION,
    "shock absorber": SUSPENSION,
    "wp - shock absorber": SUSPENSION,
    "wp - fork": SUSPENSION,
    "wp - cartridge": SUSPENSION,
    # Exhaust / body / trim
    "exhaust systems": EXHAUST,
    "exhaust street": EXHAUST,
    "trim parts": FRAME_BODY,
    "trim parts/decals": FRAME_BODY,
    "decals": DECALS,
    "decals / sticker protection": DECALS,
    "startnumber backgrounds": DECALS,
    "carbon": FRAME_BODY,
    "plastic parts set": FRAME_BODY,
    "panels & accessories": FRAME_BODY,
    "protectors": FRAME_BODY,
    "hand guards": FRAME_BODY,
    "protection": FRAME_BODY,
    "bracket": FRAME_BODY,
    "tanks": FUEL,
    "hsq - tanks": FUEL,
    "tank protection": FRAME_BODY,
    "tank bag": BAGS,
    "side bag": BAGS,
    "rear bag": BAGS,
    "inner bag": BAGS,
    "luggage bag": BAGS,
    "bags and luggage": BAGS,
    "hsq - bags and luggage": BAGS,
    "wp - bags and luggage": BAGS,
    # Wheels / windows / mirrors / electrics
    "wheels": WHEELS,
    "windshields": WINDOW,
    "windscreens": WINDOW,
    "fly screens": WINDOW,
    "instruments/electrics": ELECTRICAL,
    "electrical system / diagnosis": ELECTRICAL,
    "handlebars/instruments/electrics": CONTROLS,
    # Luggage
    "luggage cases": BAGS,
    "luggage carrier": FRAME_BODY,
    "backpacks / bags": BAGS,
    # Tools
    "special tools": TOOLS,
    "tool/transport": TOOLS,
    "fork tool": TOOLS,
    "support tool": TOOLS,
    "suspension tool": TOOLS,
    "measuring tool & setting gauge": TOOLS,
    "pressing tool": TOOLS,
    "extractor tool": TOOLS,
    "crankshaft pressing tool": TOOLS,
    "other tools": TOOLS,
    "shock absorber tool": TOOLS,
    "valve and timing tool": TOOLS,
    "bleeder tool": TOOLS,
    "hsq - bleeder tool": TOOLS,
    "hsq - tools": TOOLS,
    "bike stand / lift": TOOLS,
    "stands": TOOLS,
    "diagnosetool": TOOLS,
    "diagnosis tool": TOOLS,
    "hv tool": TOOLS,
    "wp - mounting tool": TOOLS,
    "wp - clamping stand": TOOLS,
    "wp - socket": TOOLS,
    "wp - wrench": TOOLS,
    "wp - toolboard": TOOLS,
    # Bikes / balance / complete bikes
    "electric balance bikes": BIKES_E,
    "hsq - electric balance bikes": BIKES_E,
    "bicycle": BIKES,
    "e mtb fully": BIKES_E,
    "e mtb ht": BIKES_E,
    "mtb hardtail": BIKES,
    "e tronroad": BIKES_E,
    "e city": BIKES_E,
    "sl e tronroad": BIKES_E,
    "sl e troffroad": BIKES_E,
    "sl e kids": BIKES_E,
    "sl e mtb ht": BIKES_E,
    "xc_2": BIKES,
    "display e-bike": BIKE_DISPLAYS,
    "display ebike": BIKE_DISPLAYS,
    "e-bike display": BIKE_DISPLAYS,
    "ebike display": BIKE_DISPLAYS,
    # Other
    "gift card": GIFTCARD,
    "gift cards": GIFTCARD,
    "drivetrain kit": DRIVETRAIN,
    "alarm system": ELECTRICAL,
    "navigation": ELECTRICAL,
    "smartphone case": PHONE_CASES,
    "smartphone cases": PHONE_CASES,
    "phone case": PHONE_CASES,
    "footpegs": CONTROLS,
    "side bag": BAGS,
    "rear bag": BAGS,
    "inner bag": BAGS,
    "luggage accessoires": BAGS,
    "chasis street": FRAME_BODY,
    "chassis street": FRAME_BODY,
    "hsq - electrical system / diagnosis": ELECTRICAL,
    "hsq - boots": BOOTS,
    "wp - jackets": CLOTHING,
    "wp - adaptor": TOOLS,
    "mannequin": CLOTHING,
    "chassis/triple clamp": SUSPENSION,
    "chains/sprockets": DRIVETRAIN,
    "original spare part kits": ENGINE_PARTS,
}

# Prefix rules after exact match (longest / most specific first).
TYPE_PREFIX: list[tuple[str, str]] = [
    ("bicycle first layer", BIKE_CLOTHING),
    ("bicycle gloves", BIKE_CLOTHING),
    ("bicycle jackets", BIKE_CLOTHING),
    ("bicycle jerseys", BIKE_CLOTHING),
    ("bicycle pants", BIKE_CLOTHING),
    ("bicycle shorts", BIKE_CLOTHING),
    ("bicycle socks", BIKE_CLOTHING),
    ("bicycle shoes", BIKE_CLOTHING),
    ("bicycle helmets", BIKE_HELMETS),
    ("bicycle helmet", BIKE_HELMETS),
    ("bicycle heads and scarfs", CLOTHING),
    ("bicycle sunglasses", SUNGLASSES),
    ("bicycle backpacks", BAGS),
    ("bicycle bags", BAGS),
    # E-bike subsystem types — altijd fiets, nooit Motor Vehicle Electrical.
    ("bicycle e-bike, bicycle remotes and displays", BIKE_DISPLAYS),
    ("bicycle e-bike, bicycle cables and displays", BIKE_PARTS),
    ("bicycle e-bike, bicycle batteries", BIKE_PARTS),
    ("bicycle e-bike, bicycle battery", BIKE_PARTS),
    ("bicycle e-bike, bicycle chargers", BIKE_PARTS),
    ("bicycle e-bike, biccycle smartphone case", PHONE_CASES),
    ("bicycle e-bike, bicycle smartphone case", PHONE_CASES),
    ("bicycle e-bike, bicycle batter", BIKE_PARTS),
    ("bicycle e-bike", BIKE_PARTS),
    ("bicycle ", BIKE_PARTS),
    ("display e-bike", BIKE_DISPLAYS),
    ("display ebike", BIKE_DISPLAYS),
    ("hsq - electric balance", BIKES_E),
    ("hsq - seatcover", SEATING),
    ("hsq - piston", ENGINE_PARTS),
    ("hsq - helmet", HELMETS),
    ("hsq - bags", BAGS),
    ("hsq - tank", FUEL),
    ("hsq - tool", TOOLS),
    ("hsq - bleed", TOOLS),
    ("hsq - electrical", ELECTRICAL),
    ("wp - shock", SUSPENSION),
    ("wp - fork", SUSPENSION),
    ("wp - cartridge", SUSPENSION),
    ("wp - bags", BAGS),
    ("wp - tool", TOOLS),
    ("wp - wrench", TOOLS),
    ("wp - socket", TOOLS),
    ("wp - clamp", TOOLS),
    ("wp - mount", TOOLS),
    ("sl e ", BIKES_E),
    ("e city", BIKES_E),
    ("e mtb", BIKES_E),
]

# Alleen apparel/lifestyle tags — géén PowerParts → generic MVP.
TAG_RULES: list[tuple[str, str]] = [
    ("powerwear", CLOTHING),
    ("casual and accessories", CLOTHING),
    ("casual & accessories", CLOTHING),
    ("casual", CLOTHING),
    ("electric balance bikes", BIKES_E),
    ("special tools", TOOLS),
    ("tool/transport", TOOLS),
    ("marketing material", EVENT_MATERIAL),
    ("hsq - marketing material", EVENT_MATERIAL),
]

# (label, regex on title+body lowercase, category) — first match wins.
# Specifieker dan parent Motor Vehicle Parts.
_TEXT_PATTERNS: list[tuple[str, re.Pattern[str], str]] = [
    ("text:gift card", re.compile(r"\bgift\s*card\b|cadeaubon"), GIFTCARD),
    # Fietshelm vóór generieke helm (anders Motorcycle Helmets).
    (
        "text:bike-helmet",
        re.compile(r"\b(bicycle\s*helmets?|bike\s*helmets?|fietshelm)\b"),
        BIKE_HELMETS,
    ),
    (
        "text:bag",
        re.compile(
            r"\b(tank\s*bag|side\s*bag|rear\s*bag|inner\s*bag|luggage\s*bag|"
            r"top\s*case|backpack|rucksack|hydration\s*(pack|backpack|bag))\b"
        ),
        BAGS,
    ),
    ("text:helmet", re.compile(r"\b(motorcycle\s*)?helmets?\b|\bhelm\b"), HELMETS),
    ("text:gloves", re.compile(r"\bgloves?\b|\bhandschoen"), GLOVES),
    (
        "text:boots",
        re.compile(r"\b(mx\s+boots?|motocross\s+boots?|riding\s+boots?|laarzen|\bboots?\b)\b"),
        BOOTS,
    ),
    ("text:goggles", re.compile(r"\bgoggles?\b"), GOGGLES),
    (
        "text:sunglasses",
        re.compile(
            r"\b(sunglasses?|sun\s*glasses?|shades|zonnebril|eyewear|"
            r"impact-resistant\s+glasses|uvex)\b"
        ),
        SUNGLASSES,
    ),
    (
        "text:flag",
        re.compile(r"\b(flag\s*stand|flagpole|flag\s*pole|vlaggenmast|windsock)\b"),
        FLAG_HARDWARE,
    ),
    (
        "text:apparel",
        re.compile(
            r"\b(t-?shirts?\b|tees?\b|hoodie|sweatshirt|sweat\s*jacket|jersey|"
            r"longsleeve|long\s*sleeve|beanie|polo\b|crewneck|"
            r"trousers?\b|\bpants?\b|\bshorts?\b)\b"
        ),
        CLOTHING,
    ),
    ("text:seat", re.compile(r"\b(seat\s*cover|zadel|saddle\s*cover)\b"), SEATING),
    ("text:window", re.compile(r"\b(fly\s*screen|windshield|windscreen|wind\s*screen)\b"), WINDOW),
    ("text:mirror", re.compile(r"\bmirrors?\b"), MIRRORS),
    ("text:exhaust", re.compile(r"\b(exhaust|silencer|muffler|uitlaat|header)\b"), EXHAUST),
    # Alleen echte remonderdelen — niet complete fietsen met "brake" in de specs.
    (
        "text:brake",
        re.compile(
            r"\b(brake\s*pad|brake\s*disc|brake\s*rotor|brake\s*caliper|"
            r"brake\s*lever|brake\s*line|brake\s*hose|braking\s*system|"
            r"remblok|remschijf)\b"
        ),
        BRAKING,
    ),
    ("text:clutch", re.compile(r"\bclutch\b"), DRIVETRAIN),
    ("text:chain", re.compile(r"\b(chain|sprocket|ketting|tandwiel)\b"), DRIVETRAIN),
    ("text:suspension", re.compile(r"\b(fork|shock|suspension|triple\s*clamp|swing\s*arm|swingarm|pds|damping)\b"), SUSPENSION),
    ("text:cooling", re.compile(r"\b(radiator|coolant|cooling|water\s*pump)\b"), COOLING),
    ("text:air", re.compile(r"\b(air\s*filter|airbox|intake)\b"), AIR_INTAKE),
    (
        "text:fuel",
        re.compile(r"\b(fuel\s*tank|fuel\s*pump|throttle\s*body|injector|carburett?or)\b"),
        FUEL,
    ),
    ("text:oil", re.compile(r"\b(engine\s*oil|fork\s*oil|brake\s*fluid|motorex|oil\s*filter|scottoil|\bolie\b)\b"), OIL_CIRC),
    ("text:electrical", re.compile(r"\b(battery|ecu|wiring|harness|ignition|stator|regulator|relay|sensor|cable)\b"), ELECTRICAL),
    ("text:lighting", re.compile(r"\b(headlight|taillight|turn\s*signal|led\s*light|\blamp\b|knipper)\b"), LIGHTING),
    (
        "text:engine-hw",
        re.compile(
            r"\b(piston|gasket|cylinder|crankshaft|camshaft|valve|engine|cilinder|"
            r"o-?ring|seal|bushing|bearing|shim|spring|shaft|needle)\b"
        ),
        ENGINE_PARTS,
    ),
    ("text:wheel", re.compile(r"\b(wheel|rim|tire|tyre|spoke|\bnaaf\b)\b"), WHEELS),
    (
        "text:body",
        re.compile(
            r"\b(fairing|fender|guard|protector|cover|panel|plastic\s*parts?|frame|handguard|"
            r"crash\s*bar|crash\s*bung|side\s*stand|centre\s*stand|center\s*stand|"
            r"carrier|mounting\s*kit|luggage|spoiler|bracket|clamp|footpeg|footrest|"
            r"screw|bolt|nut|washer|collar)\b"
        ),
        FRAME_BODY,
    ),
    ("text:decal", re.compile(r"\b(decal|sticker|start\s*number|number\s*plate)\b"), DECALS),
    ("text:tools", re.compile(r"\b(special\s*tool|torque\s*wrench|tool\s*kit|gereedschap|socket|wrench)\b"), TOOLS),
    (
        "text:bike",
        re.compile(r"\b(electric\s*balance|e-?bike|bicycle|fiets|stacyc)\b"),
        BIKES_E,
    ),
    ("text:controls", re.compile(r"\b(handlebar|grip|lever|throttle|stuur)\b"), CONTROLS),
]


# Types waar type te vaag is: sla type over, gebruik titel/tags.
# (Geen “text first bij geldig type” meer — Type wint altijd als die matcht.)
TEXT_FIRST_TYPES: set[str] = set()  # bewust leeg; generic types doen dit al


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
    leaf = path.split(" > ")[-1]
    if path == CLOTHING or path == BIKE_CLOTHING:
        return "Clothing"
    if path == TOOLS:
        return "Tools"
    if path in (BIKES_E, BIKES, BIKE_PARTS):
        return "Bicycles / bike parts"
    if path == BIKE_HELMETS:
        return "Bicycle Helmets"
    if path == BIKE_DISPLAYS:
        return "E-bike / bike displays"
    if path == PHONE_CASES:
        return "Phone cases"
    if path == HELMETS:
        return "Motorcycle Helmets"
    if path == GLOVES:
        return "Gloves"
    if path == BOOTS:
        return "Boots"
    if path == SHOES:
        return "Shoes"
    if path == GOGGLES:
        return "Goggles"
    if path == SUNGLASSES:
        return "Sunglasses"
    if path in (SPORT_BAGS, BAGS):
        return "Bags"
    if path == GIFTCARD:
        return "Gift cards"
    if path == DECALS:
        return "Decals / stickers"
    if path == EVENT_MATERIAL:
        return "Event / merchandising"
    if path == FLAG_HARDWARE:
        return "Flag hardware"
    if path.startswith(_MVP + " > "):
        return leaf.replace("Motor Vehicle ", "")
    if path == VEHICLE_PARTS:
        return "Motor Vehicle Parts (generic)"
    return leaf


_BRAND_TYPE_PREFIX = re.compile(r"^(hsq|wp)\s*-\s*", re.IGNORECASE)


def _type_lookup_keys(ptype_key: str) -> list[str]:
    """Volledige type-key + zonder HSQ-/WP-prefix (merk is alleen herkenning)."""
    keys = [ptype_key]
    bare = _BRAND_TYPE_PREFIX.sub("", ptype_key).strip()
    if bare and bare != ptype_key:
        keys.append(bare)
    return keys


def is_missing_shopify_category(full_name: str | None) -> bool:
    """True als Category leeg is of de placeholder Uncategorized."""
    name = (full_name or "").strip()
    if not name:
        return True
    return name.casefold() in {"uncategorized", "na"}


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

    Volgorde: specifiek Type (HSQ/WP-prefix genegeerd) → titel/body → tags → XML → default.
    Generieke types (Partstream, Archive, …) forceren géén type-hit.
    """
    ptype = (product_type or "").strip()
    ptype_key = ptype.lower()
    type_keys = _type_lookup_keys(ptype_key) if ptype_key else []
    bare_type = type_keys[-1] if type_keys else ""
    blob = f"{title or ''}\n{body_html or ''}".lower()
    blob = re.sub(r"<[^>]+>", " ", blob)

    def _from_text() -> CategoryDecision | None:
        if not blob.strip():
            return None
        for label, pattern, path in _TEXT_PATTERNS:
            if pattern.search(blob):
                return CategoryDecision(path, label, _bucket_for_path(path))
        return None

    def _from_type() -> CategoryDecision | None:
        if not ptype:
            return None
        if any(k in GENERIC_TYPES for k in type_keys):
            return None
        for key in type_keys:
            if key in TYPE_EXACT:
                path = TYPE_EXACT[key]
                return CategoryDecision(path, f"type:{ptype}", _bucket_for_path(path))
        for key in type_keys:
            for prefix, path in TYPE_PREFIX:
                if key.startswith(prefix):
                    return CategoryDecision(
                        path, f"type-prefix:{prefix.strip()}", _bucket_for_path(path)
                    )
        return None

    # Type eerst (tenzij generic) — daarna pas titel/omschrijving.
    typed = _from_type()
    if typed:
        return typed
    hit = _from_text()
    if hit:
        return hit

    if tags is None:
        tag_list: list[str] = []
    elif isinstance(tags, str):
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    else:
        tag_list = [str(t).strip() for t in tags if str(t).strip()]

    tag_keys: list[str] = []
    for tag in tag_list:
        tl = tag.lower().strip()
        tag_keys.append(tl)
        bare = _BRAND_TYPE_PREFIX.sub("", tl).strip()
        if bare and bare != tl:
            tag_keys.append(bare)
    tag_key_set = set(tag_keys)
    for needle, path in TAG_RULES:
        if needle in tag_key_set or any(needle in t for t in tag_key_set):
            return CategoryDecision(path, f"tag:{needle}", _bucket_for_path(path))

    if xml_category and xml_category.strip():
        path = map_shopify_product_category(xml_category)
        return CategoryDecision(path, f"xml:{xml_category.strip()}", _bucket_for_path(path))

    return CategoryDecision(
        DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
        "default",
        _bucket_for_path(DEFAULT_SHOPIFY_PRODUCT_CATEGORY),
    )
