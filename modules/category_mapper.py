DEFAULT_CATEGORY = "Motor Vehicle Parts"

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
