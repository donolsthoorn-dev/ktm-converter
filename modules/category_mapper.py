DEFAULT_CATEGORY = "Motor Vehicle Parts"

# Shopify admin "Category" = standard product taxonomy (full breadcrumb with " > ").
# See https://shopify.github.io/product-taxonomy/
DEFAULT_SHOPIFY_PRODUCT_CATEGORY = (
    "Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts"
)

# Values aligned with Shopify's English taxonomy (same keys as map_category() outcomes).
_SHOPIFY_PRODUCT_CATEGORY_BY_GOOGLE = {
    "Motor Vehicle Parts": DEFAULT_SHOPIFY_PRODUCT_CATEGORY,
    "Tools": "Hardware > Tools",
    "Clothing": "Apparel & Accessories > Clothing",
    "Bicycles": (
        "Sporting Goods > Outdoor Recreation > Cycling > Bicycles > Electric Bikes"
    ),
}

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


def map_category(ktm_category):

    if not ktm_category:
        return DEFAULT_CATEGORY

    return CATEGORY_MAP.get(
        ktm_category.strip(),
        DEFAULT_CATEGORY
    )


def map_shopify_product_category(ktm_category: str) -> str:
    """Full Shopify standard category path for CSV column Product category."""
    google = map_category(ktm_category)
    return _SHOPIFY_PRODUCT_CATEGORY_BY_GOOGLE.get(google, DEFAULT_SHOPIFY_PRODUCT_CATEGORY)
