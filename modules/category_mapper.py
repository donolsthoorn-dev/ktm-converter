"""
Shopify standaard-Category (taxonomie) afleiden uit Type, tags, titel en body.

Prioriteit (hoog → laag):
  1. Specifiek product Type (exact of prefix; HSQ-/WP-prefix genegeerd)
  2. Zoekwoorden in titel + omschrijving → specifieke taxonomie-tak
  3. Specifieke tags (PowerWear, Casual, …)
  4. Default: Motor Vehicle Parts (alleen als niets matcht)

Generieke types (Partstream, Archive, PowerParts, Lifestyle, …) forceren
géén type-hit; die gaan door naar titel/keywords.

shop=\"motox\" laadt modules.category_mapper_motox (Nederlandse Types) als overlay
vóór de gedeelde KTM TYPE_EXACT. Jobs: shops_category_fill_empty /
shops_category_reclassify met --shop motox.

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
MOTO_BAGS = (
    "Vehicles & Parts > Vehicle Parts & Accessories > Motorcycle Bags & Panniers"
)
HELMETS = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorcycle Protective Gear > Motorcycle Helmets"
)
GLOVES = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorcycle Protective Gear > Motorcycle Gloves"
)
RACING_SUITS = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Safety & Security > Motorsports Protective Gear > Racing Suits"
)
MOTORCYCLE_OUTERWEAR = (
    "Apparel & Accessories > Clothing > Outerwear > Motorcycle Outerwear"
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
# Let op: Shopify heeft "Bicycle Parts", géén "Bicycle Parts & Accessories"
# (verkeerd pad → apply viel stil terug op Motor Vehicle Parts).
BIKE_PARTS = "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Parts"
BIKE_ACCESSORIES = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Accessories"
)
BIKE_HELMETS = (
    "Sporting Goods > Outdoor Recreation > Cycling > "
    "Cycling Apparel & Accessories > Bicycle Helmets"
)
BIKE_DISPLAYS = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Accessories > "
    "Bicycle Computer Accessories > Bicycle Computer Displays"
)
BIKE_BAGS = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Accessories > "
    "Bicycle Bags & Panniers > Bicycle Bags"
)
BIKE_BASKETS = (
    "Sporting Goods > Outdoor Recreation > Cycling > Bicycle Accessories > "
    "Bicycle Baskets"
)
BIKE_LIGHTS = f"{BIKE_ACCESSORIES} > Bicycle Lights"
BIKE_LOCKS = f"{BIKE_ACCESSORIES} > Bicycle Locks"
BIKE_MIRRORS = f"{BIKE_ACCESSORIES} > Bicycle Mirrors"
BIKE_PUMPS = f"{BIKE_ACCESSORIES} > Bicycle Pumps"
BIKE_CAGES = f"{BIKE_ACCESSORIES} > Bicycle Cages"
BIKE_BOTTLE_CAGES = f"{BIKE_CAGES} > Bicycle Bottle Cages"
BIKE_BELLS = f"{BIKE_ACCESSORIES} > Bicycle Bells & Horns"
BIKE_FENDERS = f"{BIKE_ACCESSORIES} > Bicycle Fenders"
BIKE_COMPUTERS = f"{BIKE_ACCESSORIES} > Bicycle Computers"
BIKE_TOOLS = f"{BIKE_ACCESSORIES} > Bicycle Tools"
BIKE_TRAILERS = f"{BIKE_ACCESSORIES} > Bicycle Trailers"
BIKE_TRAINERS = f"{BIKE_ACCESSORIES} > Bicycle Trainers"
BIKE_TRAINING_WHEELS = f"{BIKE_ACCESSORIES} > Bicycle Training Wheels"
BIKE_CHILD_SEATS = f"{BIKE_ACCESSORIES} > Bicycle Child Seats"
BIKE_RACKS = f"{BIKE_ACCESSORIES} > Bicycle Front & Rear Racks"
BIKE_STANDS = f"{BIKE_ACCESSORIES} > Bicycle Stands & Storage"
BIKE_GRIPS = f"{BIKE_ACCESSORIES} > Bicycle Handlebar Grips & Decor"
BIKE_WATER_BOTTLES = (
    "Home & Garden > Kitchen & Dining > Food & Beverage Carriers > Water Bottles"
)
# Specifieke onderdelen-leaves
BIKE_WHEELS = f"{BIKE_PARTS} > Bicycle Wheels"
BIKE_TIRES = f"{BIKE_PARTS} > Bicycle Tires"
BIKE_TUBES = f"{BIKE_PARTS} > Bicycle Tubes"
BIKE_FORKS = f"{BIKE_PARTS} > Bicycle Forks"
BIKE_FRAMES = f"{BIKE_PARTS} > Bicycle Frames"
BIKE_HANDLEBARS = f"{BIKE_PARTS} > Bicycle Handlebars"
BIKE_HEADSETS = f"{BIKE_PARTS} > Bicycle Headsets"
BIKE_SADDLES = f"{BIKE_PARTS} > Bicycle Saddles"
BIKE_SEATPOSTS = f"{BIKE_PARTS} > Bicycle Seatposts"
BIKE_STEMS = f"{BIKE_PARTS} > Bicycle Stems"
BIKE_KICKSTANDS = f"{BIKE_PARTS} > Bicycle Kickstands"
BIKE_BRAKE_PARTS = f"{BIKE_PARTS} > Bicycle Brake Parts"
BIKE_DRIVETRAIN = f"{BIKE_PARTS} > Bicycle Drivetrain Parts"
BIKE_CHAINS = f"{BIKE_DRIVETRAIN} > Bicycle Chains"
BIKE_PEDALS = f"{BIKE_DRIVETRAIN} > Bicycle Pedals"
BIKE_CRANKS = f"{BIKE_DRIVETRAIN} > Bicycle Cranks"
BIKE_CASSETTES = f"{BIKE_DRIVETRAIN} > Bicycle Cassettes & Freewheels"
BIKE_BOTTOMS = f"{BIKE_DRIVETRAIN} > Bicycle Bottom Brackets"
BIKE_CHAINRINGS = f"{BIKE_DRIVETRAIN} > Bicycle Chainrings"
CYCLING_APPAREL = (
    "Sporting Goods > Outdoor Recreation > Cycling > Cycling Apparel & Accessories"
)
BIKE_SHOES = f"{CYCLING_APPAREL} > Cycling Shoes"
BIKE_JERSEYS = f"{CYCLING_APPAREL} > Cycling Jerseys"
BIKE_SHORTS = f"{CYCLING_APPAREL} > Cycling Shorts & Bib Shorts"
BIKE_JACKETS = f"{CYCLING_APPAREL} > Cycling Jackets & Vests"
BIKE_HEADWEAR = f"{CYCLING_APPAREL} > Cycling Caps & Headwear"
BIKE_CLOTHING = CYCLING_APPAREL
PHONE_CASES = (
    "Electronics > Communications > Telephony > "
    "Mobile & Smart Phone Accessories > Mobile Phone Cases"
)
GIFTCARD = "Arts & Entertainment > Party & Celebration > Gift Giving > Gift Cards"
OILS = OIL_CIRC  # dichtstbijzijnde taxonomie voor olie/vloeistoffen
# Motorstickers/graphics → Vehicle Decals (niet Toys; beter voor Merchant Center).
DECALS = (
    "Vehicles & Parts > Vehicle Parts & Accessories > "
    "Vehicle Maintenance, Care & Decor > Vehicle Decor > Vehicle Decals"
)
# Event / merchandising (geen motoronderdelen).
EVENT_MATERIAL = (
    "Business & Industrial > Advertising & Marketing > Trade Show Displays"
)
FLAG_HARDWARE = (
    "Home & Garden > Lawn & Garden > Outdoor Living > Outdoor Structures > "
    "Flags & Windsocks > Flag & Windsock Accessories > "
    "Flag & Windsock Pole Mounting Hardware & Kits"
)
CLEANERS = (
    "Home & Garden > Household Supplies > Household Cleaning Supplies > "
    "Household Cleaning Products"
)
GLUE = "Hardware > Building Consumables > Hardware Glue & Adhesives"
# Zelfde leaf als fietsflessen; merch-drinkfles ≠ Clothing.
DRINK_BOTTLES = BIKE_WATER_BOTTLES
# Motorkrik / paddockstand (niet side/centre stand op de motor)
MOTO_STANDS = TOOLS

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
    # Te vaag / catalogus-bakken zonder zinvolle leaf
    "new",
    "other",
    "kids",
    "flash",
    "best deal",
    "hsq -",
    # XC_2 = productlijn (vaak elektrisch/diagnose), geen fiets — titel beslist.
    "xc_2",
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
    # Accessoires: zie SOFT_TYPE_FALLBACKS (fles/cleaner/lijm via tekst, anders kleding).
    "other accessoires": EVENT_MATERIAL,
    "other accessories": EVENT_MATERIAL,
    "racetrack and camping": EVENT_MATERIAL,
    "jerseys": CLOTHING,
    "shirts": CLOTHING,
    # "backpacks" bewust niet exact: KTM-fietstassen staan soms als Backpacks;
    # laat text:bike-bag / text:bag beslissen.
    "bicycle bags": BIKE_BAGS,
    "bicycle baskets": BIKE_BASKETS,
    "bicycle workwear": CYCLING_APPAREL,
    "bicycle work wear": CYCLING_APPAREL,
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
    "graphics & stickers": DECALS,
    "graphics and stickers": DECALS,
    "exhaust stickers": DECALS,
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
    "tank bag": MOTO_BAGS,
    "side bag": MOTO_BAGS,
    "rear bag": MOTO_BAGS,
    "inner bag": MOTO_BAGS,
    "luggage bag": MOTO_BAGS,
    "bags and luggage": MOTO_BAGS,
    "hsq - bags and luggage": MOTO_BAGS,
    "wp - bags and luggage": MOTO_BAGS,
    # Wheels / windows / mirrors / electrics
    "wheels": WHEELS,
    "windshields": WINDOW,
    "windscreens": WINDOW,
    "fly screens": WINDOW,
    "instruments/electrics": ELECTRICAL,
    "electrical system / diagnosis": ELECTRICAL,
    "handlebars/instruments/electrics": CONTROLS,
    # Luggage
    "luggage cases": MOTO_BAGS,
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
    "bike stand / lift": MOTO_STANDS,
    "stand": MOTO_STANDS,
    # "stands" → SOFT (centre/side stand via tekst, anders paddockstand/tools)
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
    "luggage accessoires": MOTO_BAGS,
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
    # --- Bulk unmapped types (type-map CSV 20260915) ---
    # Apparel / lifestyle
    "underwear": CLOTHING,
    "leather suits": RACING_SUITS,
    "leathersuits": RACING_SUITS,
    "cadeaubonnen": GIFTCARD,
    # Brakes
    "brake pads": BRAKING,
    "brake parts": BRAKING,
    "brake calipers": BRAKING,
    "brake cylinders": BRAKING,
    "brake protection": BRAKING,
    # Drivetrain / controls
    "sprockets": DRIVETRAIN,
    "chains": DRIVETRAIN,
    "clutch kit": DRIVETRAIN,
    "chainguides and sliders": DRIVETRAIN,
    "chain protection": DRIVETRAIN,
    "grips": CONTROLS,
    "grip set": CONTROLS,
    "handlebars": CONTROLS,
    "handlebar supports": CONTROLS,
    "xtrig bar mounts": CONTROLS,
    # Engine
    "valve kit": ENGINE_PARTS,
    "cylinder seal set": ENGINE_PARTS,
    "cnc engine": ENGINE_PARTS,
    "shim kit": ENGINE_PARTS,
    "carburetor jet kit": FUEL,
    "oil filter kit": OIL_CIRC,
    "fork oil": OIL_CIRC,
    "shock absorber oil": OIL_CIRC,
    "grease": OIL_CIRC,
    "loctite": OIL_CIRC,
    "consumables": OIL_CIRC,
    # Exhaust / intake / cooling
    "exhaust parts kit": EXHAUST,
    "noise reduction": EXHAUST,
    "air filter standaard ktm": AIR_INTAKE,
    "air filter standaard": AIR_INTAKE,
    "air filter pre-oiled ktm": AIR_INTAKE,
    "pre-oiled air filter": AIR_INTAKE,
    "air filter cover": AIR_INTAKE,
    "radiator": COOLING,
    "radiator fan": COOLING,
    "radiator fans": COOLING,
    "radiator protection": COOLING,
    "hoses": COOLING,
    # Suspension / chassis / frame
    "spring": SUSPENSION,
    "lowering kit": SUSPENSION,
    "preload adjuster": SUSPENSION,
    "triple clamp": SUSPENSION,
    "xtrig triple clamps": SUSPENSION,
    "steering dampers": SUSPENSION,
    "fork/shock protection": SUSPENSION,
    "reducing ring": SUSPENSION,
    "chassis": FRAME_BODY,
    "chassis offroad": FRAME_BODY,
    "chasis offroad": FRAME_BODY,
    "cnc chassis": FRAME_BODY,
    "plastic kits": FRAME_BODY,
    "plastic parts kit": FRAME_BODY,
    "carbon parts street": FRAME_BODY,
    "carbon parts offroad": FRAME_BODY,
    "carbon protection": FRAME_BODY,
    "frame protection": FRAME_BODY,
    "heat protection": FRAME_BODY,
    "hand protection": FRAME_BODY,
    "crash bar kits": FRAME_BODY,
    "engine guard, street": FRAME_BODY,
    "engine guard, 2-stroke offroad": FRAME_BODY,
    "skid plate, 4-stroke offroad": FRAME_BODY,
    "skid plate, 2-stroke offroad": FRAME_BODY,
    "skid plate, street": FRAME_BODY,
    "swingarm protection": FRAME_BODY,
    "protecting sleeves & protection caps": FRAME_BODY,
    "headlight protection": LIGHTING,
    # Wheels
    "wheels haan rear": WHEELS,
    "wheels haan front": WHEELS,
    "wheels accessoires": WHEELS,
    "wheels graphic": WHEELS,
    "wheel repair kit": WHEELS,
    "wheel sliders": WHEELS,
    "wheel bearing protection": WHEELS,
    "wheel set": WHEELS,
    "special parts wheels": WHEELS,
    # Electrics / ECU
    "tuning/ecu": ELECTRICAL,
    # Luggage / bags
    "luggage": MOTO_BAGS,
    "backpacks": BAGS,
    # Stickers / merch / POS
    "stickers": DECALS,
    "off-road stickerset": DECALS,
    "poster": EVENT_MATERIAL,
    "folder": EVENT_MATERIAL,
    "pricelists": EVENT_MATERIAL,
    "hangers": EVENT_MATERIAL,
    "model bike": EVENT_MATERIAL,
    "transport": EVENT_MATERIAL,
    "transportation": EVENT_MATERIAL,
    # Tools / stands
    "piston tool": TOOLS,
    "transmission tool": TOOLS,
    "groove nut wrench": TOOLS,
    "pliers": TOOLS,
    "spark plug wrench": TOOLS,
    "mounting tool": TOOLS,
    "measuring tool": TOOLS,
    "filling tool": TOOLS,
    "adjuster tool": TOOLS,
    "vacuum pump": TOOLS,
    "air pump": TOOLS,
    "flow meter": TOOLS,
    "socket": TOOLS,
    "screwdriver": TOOLS,
    "needle": TOOLS,
    "lift and work stand": TOOLS,
    "factory start": TOOLS,
    "drift": TOOLS,
    # Bikes (incl. NBSP-varianten worden genormaliseerd)
    "mtb fully": BIKES,
    "trekking onroad": BIKES,
    "trekking offroad": BIKES,
    "e troffroad": BIKES_E,
    "sl e gravel": BIKES_E,
    "sl e mtb fully": BIKES_E,
    "sl e road": BIKES_E,
    "stacyc electric balance bikes": BIKES_E,
    # Vage onderdelen-bakken → generieke motor parts (bewust)
    # 2-/4-stroke Offroad: zie SOFT_TYPE_FALLBACKS (tekst eerst, default Exhaust).
    "husaberg": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    "special parts": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    "spare parts functional": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    "functional spareparts": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    'spareparts 20"': DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    'spareparts 12&16"': DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    "powersale": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
}

# Prefix rules after exact match (longest / most specific first).
TYPE_PREFIX: list[tuple[str, str]] = [
    ("bicycle first layer", CYCLING_APPAREL),
    ("bicycle gloves", CYCLING_APPAREL),
    ("bicycle jackets", BIKE_JACKETS),
    ("bicycle jerseys", BIKE_JERSEYS),
    ("bicycle pants", BIKE_SHORTS),
    ("bicycle shorts", BIKE_SHORTS),
    ("bicycle socks", CYCLING_APPAREL),
    ("bicycle shoes", BIKE_SHOES),
    ("bicycle helmets", BIKE_HELMETS),
    ("bicycle helmet", BIKE_HELMETS),
    ("bicycle heads and scarfs", BIKE_HEADWEAR),
    ("bicycle workwear", CYCLING_APPAREL),
    ("bicycle work wear", CYCLING_APPAREL),
    ("bicycle innerpants", CYCLING_APPAREL),
    ("bicycle warmer", CYCLING_APPAREL),
    ("bicycle insole", BIKE_SHOES),
    ("bicycle sunglasses", SUNGLASSES),
    ("bicycle backpacks", BIKE_BAGS),
    ("bicycle bags", BIKE_BAGS),
    ("bicycle baskets", BIKE_BASKETS),
    # Accessoires met eigen leaf
    ("bicycle lights", BIKE_LIGHTS),
    ("bicycle light", BIKE_LIGHTS),
    ("bicycle locks", BIKE_LOCKS),
    ("bicycle lock", BIKE_LOCKS),
    ("bicycle mirrors", BIKE_MIRRORS),
    ("bicycle mirror", BIKE_MIRRORS),
    ("bicycle pumps", BIKE_PUMPS),
    ("bicycle pump", BIKE_PUMPS),
    ("bicycle bottle cages", BIKE_BOTTLE_CAGES),
    ("bicycle bottle cage", BIKE_BOTTLE_CAGES),
    ("bicycle bottlecages", BIKE_BOTTLE_CAGES),
    ("bicycle bottles", BIKE_WATER_BOTTLES),
    ("bicycle bottle", BIKE_WATER_BOTTLES),
    ("bicycle cages", BIKE_CAGES),
    ("bicycle bells", BIKE_BELLS),
    ("bicycle fenders", BIKE_FENDERS),
    ("bicycle computers", BIKE_COMPUTERS),
    ("bicycle computer", BIKE_COMPUTERS),
    ("bicycle tools", BIKE_TOOLS),
    ("bicycle tool", BIKE_TOOLS),
    ("bicycle bike trailers", BIKE_TRAILERS),
    ("bicycle trailers", BIKE_TRAILERS),
    ("bicycle hometrainers", BIKE_TRAINERS),
    ("bicycle trainers", BIKE_TRAINERS),
    ("bicycle kids training wheels", BIKE_TRAINING_WHEELS),
    ("bicycle training wheels", BIKE_TRAINING_WHEELS),
    ("bicycle children seat", BIKE_CHILD_SEATS),
    ("bicycle child seat", BIKE_CHILD_SEATS),
    ("bicycle carriers", BIKE_RACKS),
    ("bicycle carrier", BIKE_RACKS),
    ("bicycle kickstands", BIKE_KICKSTANDS),
    ("bicycle kickstand", BIKE_KICKSTANDS),
    ("bicycle storage", BIKE_STANDS),
    ("bicycle grips", BIKE_GRIPS),
    ("bicycle grip", BIKE_GRIPS),
    ("bicycle youngster", BIKES),
    ("bicycle bike care", BIKE_ACCESSORIES),
    ("bicycle safety", BIKE_ACCESSORIES),
    ("bicycle chains", BIKE_CHAINS),
    ("bicycle chain", BIKE_CHAINS),
    ("bicycle pedals", BIKE_PEDALS),
    ("bicycle pedal", BIKE_PEDALS),
    ("bicycle cassettes", BIKE_CASSETTES),
    ("bicycle bottom brackets", BIKE_BOTTOMS),
    ("bicycle chainguards", BIKE_PARTS),
    ("bicycle chainguides", BIKE_PARTS),
    # E-bike subsystem types — altijd fiets, nooit Motor Vehicle Electrical.
    ("bicycle e-bike, bicycle remotes and displays", BIKE_DISPLAYS),
    ("bicycle e-bike, bicycle cables and displays", BIKE_PARTS),
    ("bicycle e-bike, bicycle batteries", BIKE_PARTS),
    ("bicycle e-bike, bicycle battery", BIKE_PARTS),
    ("bicycle e-bike, bicycle chargers", BIKE_PARTS),
    ("bicycle e-bike, bicycle chainrings", BIKE_CHAINRINGS),
    ("bicycle e-bike, bicycle cranks", BIKE_CRANKS),
    ("bicycle e-bike, biccycle smartphone case", PHONE_CASES),
    ("bicycle e-bike, bicycle smartphone case", PHONE_CASES),
    ("bicycle e-bike, bicycle batter", BIKE_PARTS),
    ("bicycle e-bike", BIKE_PARTS),
    ("bicycle batteries", BIKE_PARTS),
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
    (
        "text:cleaner",
        re.compile(
            r"\b(power\s*cleaner|bike\s*cleaner|chain\s*cleaner|reiniger|"
            r"schoonmaak|cleaner|degreaser|ontvetter)\b"
        ),
        CLEANERS,
    ),
    (
        "text:glue",
        re.compile(r"\b(power\s*glue|\bglue\b|\blijm\b|adhesive|sealant|afdicht)\b"),
        GLUE,
    ),
    (
        "text:drink-bottle",
        re.compile(
            r"\b(aluminium\s*bottle|aluminum\s*bottle|drinkfles|water\s*bottle|"
            r"drinking\s*bottle)\b"
        ),
        DRINK_BOTTLES,
    ),
    # Fietshelm vóór generieke helm (anders Motorcycle Helmets).
    (
        "text:bike-helmet",
        re.compile(r"\b(bicycle\s*helmets?|bike\s*helmets?|fietshelm)\b"),
        BIKE_HELMETS,
    ),
    (
        "text:bike-bag",
        re.compile(
            r"\b(hbar\s*bag|handlebar\s*bag|trunk\s*bag|carrbag|carrier\s*bag|"
            r"pannier|frame\s*bag|saddle\s*bag|bicycle\s*bag)\b"
        ),
        BIKE_BAGS,
    ),
    (
        "text:bike-basket",
        re.compile(r"\b(bicycle\s*basket|bike\s*basket|\bbasket\b)\b"),
        BIKE_BASKETS,
    ),
    (
        "text:bag",
        re.compile(
            r"\b(tank\s*bag|side\s*bag|rear\s*bag|inner\s*bag|luggage\s*bag|"
            r"touring\s*case|top\s*case|topcase|pannier|"
            r"topcase\s*fitting|top\s*case\s*fitting|case\s*carrier|"
            r"top\s*case\s*carrier|luggage\s*case|"
            r"backpack|rucksack|hydration\s*(pack|backpack|bag))\b"
        ),
        MOTO_BAGS,
    ),
    ("text:helmet", re.compile(r"\b(motorcycle\s*)?helmets?\b|\bhelm\b"), HELMETS),
    ("text:gloves", re.compile(r"\bgloves?\b|\bhandschoen"), GLOVES),
    (
        "text:boots",
        # Geen losse "boot(s)" — anders Frame Protection / body parts → Boots.
        re.compile(r"\b(mx\s+boots?|motocross\s+boots?|riding\s+boots?|enduro\s+boots?|laarzen)\b"),
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
            r"\b(t-?shirts?\b|\btees?\b|hoodie|sweatshirt|sweat\s*jacket|jersey|"
            r"longsleeve|long\s*sleeve|beanie|\bpolo\b|crewneck|"
            r"trousers?\b|\bpants\b|\bshorts?\b)\b"
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
    ("text:electrical", re.compile(r"\b(battery|ecu|cdi|wiring|harness|ignition|stator|regulator|relay|sensor|cable|control\s*unit)\b"), ELECTRICAL),
    ("text:lighting", re.compile(r"\b(headlight|taillight|turn\s*signal|led\s*light|\blamp\b|knipper)\b"), LIGHTING),
    (
        "text:engine-hw",
        re.compile(
            # Geen losse "spring" (Exhaust spring → vals Engine Parts).
            r"\b(piston|gasket|cylinder|crankshaft|camshaft|valve|engine|cilinder|"
            r"o-?ring|seal|bushing|bearing|shim|shaft|needle)\b"
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
    ("text:decal", re.compile(r"\b(decal|sticker|start\s*number|number\s*plate|graphic\s*kit)\b"), DECALS),
    ("text:tools", re.compile(r"\b(special\s*tool|torque\s*wrench|tool\s*kit|gereedschap|socket|wrench)\b"), TOOLS),
    (
        "text:bike",
        re.compile(r"\b(electric\s*balance|e-?bike|bicycle|fiets|stacyc)\b"),
        BIKES_E,
    ),
    ("text:controls", re.compile(r"\b(handlebar|grip|lever|throttle|stuur)\b"), CONTROLS),
]


# Types waar titel/body eerst mag winnen; anders vaste fallback (niet generiek MVP).
SOFT_TYPE_FALLBACKS: dict[str, str] = {
    # Catalogusbakken met vooral uitlaten; CDI e.d. winnen via tekst.
    "2-stroke offroad": EXHAUST,
    "4-stroke offroad": EXHAUST,
    # Centre/side stand in titel → FRAME_BODY; anders paddockstand.
    "stands": MOTO_STANDS,
    # Fles/cleaner/lijm via tekst; merch-accessoire default kleding.
    "accessoires": CLOTHING,
}


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
    if path == CLOTHING:
        return "Clothing"
    if path in (CYCLING_APPAREL, BIKE_CLOTHING, BIKE_JERSEYS, BIKE_SHORTS, BIKE_JACKETS, BIKE_HEADWEAR):
        return "Cycling apparel"
    if path == BIKE_SHOES:
        return "Cycling shoes"
    if path == TOOLS:
        return "Tools"
    if path in (BIKES_E, BIKES, BIKE_PARTS, BIKE_ACCESSORIES):
        return "Bicycles / bike parts"
    if path in (DRINK_BOTTLES, BIKE_WATER_BOTTLES):
        return "Drink bottles"
    if path in (
        BIKE_LIGHTS,
        BIKE_LOCKS,
        BIKE_MIRRORS,
        BIKE_PUMPS,
        BIKE_CAGES,
        BIKE_BOTTLE_CAGES,
        BIKE_BELLS,
        BIKE_FENDERS,
        BIKE_COMPUTERS,
        BIKE_TOOLS,
        BIKE_TRAILERS,
        BIKE_TRAINERS,
        BIKE_TRAINING_WHEELS,
        BIKE_CHILD_SEATS,
        BIKE_RACKS,
        BIKE_STANDS,
        BIKE_GRIPS,
    ):
        return "Bicycle accessories"
    if path.startswith(BIKE_PARTS + " > "):
        return "Bicycle parts"
    if path == BIKE_HELMETS:
        return "Bicycle Helmets"
    if path == BIKE_DISPLAYS:
        return "E-bike / bike displays"
    if path == BIKE_BAGS:
        return "Bicycle Bags"
    if path == BIKE_BASKETS:
        return "Bicycle Baskets"
    if path == PHONE_CASES:
        return "Phone cases"
    if path == HELMETS:
        return "Motorcycle Helmets"
    if path == RACING_SUITS:
        return "Racing suits"
    if path == MOTORCYCLE_OUTERWEAR:
        return "Motorcycle outerwear"
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
    if path == MOTO_BAGS:
        return "Motorcycle bags"
    if path in (SPORT_BAGS, BAGS):
        return "Bags"
    if path == GIFTCARD:
        return "Gift cards"
    if path == DECALS:
        return "Vehicle decals / stickers"
    if path == CLEANERS:
        return "Cleaning products"
    if path == GLUE:
        return "Glue / adhesives"
    if path == DRINK_BOTTLES:
        return "Drink bottles"
    if path == EVENT_MATERIAL:
        return "Event / merchandising"
    if path == FLAG_HARDWARE:
        return "Flag hardware"
    if path.startswith(_MVP + " > "):
        return leaf.replace("Motor Vehicle ", "")
    if path == VEHICLE_PARTS:
        return "Motor Vehicle Parts (generic)"
    return leaf


# Soft lifestyle/apparel text hits — niet gebruiken bij Partstream/spareparts.
# Wel bags/tassen: Partstream-titels als "Side bag", "Touring case", "Topcase".
_PARTS_GENERIC_SKIP_TEXT = frozenset(
    {
        "text:apparel",
        "text:sunglasses",
        "text:gift card",
        "text:flag",
        "text:bike-bag",
        "text:bike-basket",
        "text:bike",
        "text:bike-helmet",
        "text:boots",
        "text:drink-bottle",
    }
)
_PARTS_GENERIC_TYPES = frozenset(
    {
        "partstream",
        "powerparts",
        "spareparts",
        "spare parts",
        "spareparts functional",
    }
)


_BRAND_TYPE_PREFIX = re.compile(r"^(hsq|wp)\s*-\s*", re.IGNORECASE)


def _type_lookup_keys(ptype_key: str) -> list[str]:
    """Volledige type-key + zonder HSQ-/WP-prefix (merk is alleen herkenning)."""
    # Shopify/XML types bevatten soms NBSP (E\xa0MTB\xa0Fully) → normaliseren.
    key = (ptype_key or "").replace("\xa0", " ").replace("\u202f", " ")
    key = re.sub(r"\s+", " ", key).strip().lower()
    keys = [key] if key else []
    bare = _BRAND_TYPE_PREFIX.sub("", key).strip()
    if bare and bare not in keys:
        keys.append(bare)
    return keys


def is_missing_shopify_category(full_name: str | None) -> bool:
    """True als Category leeg is of de placeholder Uncategorized."""
    name = (full_name or "").strip()
    if not name:
        return True
    return name.casefold() in {"uncategorized", "na"}


def _shop_type_overlays(shop: str | None) -> tuple[dict[str, str], list[tuple[str, str]], set[str], dict[str, str]]:
    """Return (exact, prefix, generic, soft) overlays for shop; empty for ktm/default."""
    if (shop or "").strip().lower() != "motox":
        return {}, [], set(), {}
    # Lazy import: Motox-module hangt van path-constanten in dit bestand af.
    from modules.category_mapper_motox import (  # noqa: WPS433
        GENERIC_TYPES_MOTOX,
        SOFT_TYPE_FALLBACKS_MOTOX,
        TYPE_EXACT_MOTOX,
        TYPE_PREFIX_MOTOX,
    )

    return (
        TYPE_EXACT_MOTOX,
        TYPE_PREFIX_MOTOX,
        GENERIC_TYPES_MOTOX,
        SOFT_TYPE_FALLBACKS_MOTOX,
    )


def resolve_shopify_product_category(
    *,
    product_type: str | None = None,
    tags: str | list[str] | None = None,
    title: str | None = None,
    body_html: str | None = None,
    xml_category: str | None = None,
    shop: str | None = None,
) -> CategoryDecision:
    """
    Bepaal Shopify Category-pad + bron.

    Volgorde: specifiek Type (HSQ/WP-prefix genegeerd) → titel/body → tags → XML → default.
    Generieke types (Partstream, Archive, …) forceren géén type-hit.
    shop=\"motox\" activeert de Nederlandse Motox type-overlay vóór gedeelde KTM-maps.
    """
    ptype = (product_type or "").strip()
    ptype_key = ptype.lower()
    type_keys = _type_lookup_keys(ptype_key) if ptype_key else []
    bare_type = type_keys[-1] if type_keys else ""
    blob = f"{title or ''}\n{body_html or ''}".lower()
    blob = re.sub(r"<[^>]+>", " ", blob)

    motox_exact, motox_prefix, motox_generic, motox_soft = _shop_type_overlays(shop)
    type_exact = {**TYPE_EXACT, **motox_exact} if motox_exact else TYPE_EXACT
    # Motox-prefixes eerst (specifieker Nederlands), daarna gedeelde.
    type_prefix = list(motox_prefix) + list(TYPE_PREFIX) if motox_prefix else TYPE_PREFIX
    generic_types = GENERIC_TYPES | motox_generic
    soft_fallbacks = {**SOFT_TYPE_FALLBACKS, **motox_soft}

    parts_generic = any(k in _PARTS_GENERIC_TYPES for k in type_keys)

    def _from_text() -> CategoryDecision | None:
        if not blob.strip():
            return None
        for label, pattern, path in _TEXT_PATTERNS:
            if parts_generic and label in _PARTS_GENERIC_SKIP_TEXT:
                continue
            if pattern.search(blob):
                return CategoryDecision(path, label, _bucket_for_path(path))
        return None

    def _from_type() -> CategoryDecision | None:
        if not ptype:
            return None
        if any(k in generic_types for k in type_keys):
            return None
        for key in type_keys:
            if key in type_exact:
                path = type_exact[key]
                return CategoryDecision(path, f"type:{ptype}", _bucket_for_path(path))
        for key in type_keys:
            for prefix, path in type_prefix:
                if key.startswith(prefix):
                    return CategoryDecision(
                        path, f"type-prefix:{prefix.strip()}", _bucket_for_path(path)
                    )
        return None

    # Soft types (2-/4-stroke Offroad / Motox vage bakken): tekst eerst, anders fallback.
    soft_fallback = None
    for key in type_keys:
        if key in soft_fallbacks:
            soft_fallback = soft_fallbacks[key]
            break
    if soft_fallback is not None:
        hit = _from_text()
        if hit:
            return hit
        return CategoryDecision(
            soft_fallback,
            f"type-fallback:{ptype}",
            _bucket_for_path(soft_fallback),
        )

    # Type eerst (tenzij generic) — daarna pas titel/omschrijving.
    typed = _from_type()
    if typed:
        return typed
    hit = _from_text()
    if hit:
        return hit

    # Partstream/spareparts: tags overslaan (vaak PowerWear/Casual vals) → default motor parts.
    if parts_generic:
        return CategoryDecision(
            DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
            "default:partstream",
            _bucket_for_path(DEFAULT_SHOPIFY_PRODUCT_CATEGORY),
        )

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
