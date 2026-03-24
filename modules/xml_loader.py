import re
import html
import json
import os
from collections import defaultdict
from functools import lru_cache
from lxml import etree
from config import XML_FILE, CULTURE, INPUT_DIR

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
SIZE_RE = re.compile(r"^(XXXS|XXS|XS|S|M|L|XL|XXL|XXXL|XXXXL)$", re.IGNORECASE)
LANG_TOKEN_RE = re.compile(
    r"(DE|EN|FR|ES|IT|NL|PT|CZ|SK|HU|PL|DK|GR|FIN|SWE|EST|LAT|LIT|SLK|FRA|AT|CH|BE|LUX)$",
    re.IGNORECASE,
)

# -----------------------------------------------------
# Helpers
# -----------------------------------------------------

def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def strip_language_suffix(sku: str) -> str:
    s = sku.strip()
    if not s:
        return s

    while True:
        base = re.sub(r"([/-](DE|EN|FR|ES|IT|NL|PT|CZ|SK|HU|PL|DK|GR|FIN|SWE|EST|LAT|LIT|SLK|FRA|AT|CH|BE|LUX))$", "", s, flags=re.IGNORECASE)
        if base == s:
            break
        s = base

    return re.sub(
        r"(DE|EN|FR|ES|IT|NL|PT|CZ|SK|HU|PL|DK|GR|FIN|SWE|EST|LAT|LIT|SLK|FRA|AT|CH|BE|LUX)$",
        "",
        s,
        flags=re.IGNORECASE,
    )


@lru_cache(maxsize=1)
def load_handle_overrides():
    path = os.path.join(INPUT_DIR, "handle-overrides.json")

    if not os.path.exists(path):
        return {"keys": {}, "skus": {}}

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            return {
                "keys": data.get("keys", {}) or {},
                "skus": data.get("skus", {}) or {},
            }
    except Exception as e:
        print(f"Kon handle-overrides niet lezen: {e}")
        return {"keys": {}, "skus": {}}

def first_text(nodes):
    for n in nodes:
        if n is not None and n.text and n.text.strip():
            return n.text.strip()
    return ""

def get_html_textart(elem, name):
    raw = first_text(elem.xpath(
        f'.//TEXTART[@name="{name}"]/TEXT[@culture="{CULTURE}"]'
    ))
    return html.unescape(raw) if raw else ""


def textart_lines(elem, name):
    lines = []
    for t in elem.xpath(f'.//TEXTART[@name="{name}"]/TEXT[@culture="{CULTURE}"]'):
        val = (t.text or "").strip()
        if val:
            lines.append(html.unescape(val))
    return lines


def build_description(elem):
    # Prefer the richest HTML fields first.
    for name in ("BESCHRTEXT_ALG", "BESCHRTEXT_GEN_D", "BESCHRTEXT_GEN"):
        text = get_html_textart(elem, name)
        if text:
            return text

    # Fallback: plain feature lines -> HTML list.
    lines = textart_lines(elem, "BESCHRTEXT_EIGENSCH")
    if lines:
        items = "".join(f"<li>{html.escape(line)}</li>" for line in lines)
        return f"<ul>{items}</ul>"

    return ""


def find_group_attr_value(skus: list[str], sku_attrs: dict, attr_names: list[str]) -> str:
    for sku in skus:
        attrs = sku_attrs.get(sku, {})
        for name in attr_names:
            val = (attrs.get(name) or "").strip()
            if val:
                return val
    return ""


def build_properties_table_html(skus: list[str], sku_attrs: dict) -> str:
    rows = []

    color = find_group_attr_value(skus, sku_attrs, ["COLOUR_POWERWEAR", "COLOUR", "COLOR"])
    if color:
        rows.append(("Colour", color.lower()))

    gender = find_group_attr_value(skus, sku_attrs, ["PW_GENDER", "GENDER"])
    if gender:
        rows.append(("Gender", gender))

    collection = find_group_attr_value(skus, sku_attrs, ["PW_KTM_COLL", "KTM_COLLECTION"])
    if collection:
        rows.append(("KTM Collection", collection))

    playground = find_group_attr_value(skus, sku_attrs, ["PW_KTM_PLAY", "KTM_PLAYGROUND"])
    if playground:
        rows.append(("KTM Playground", playground))

    if not rows:
        return ""

    body = "".join(
        f'<tr class="product--properties-row"><td class="product--properties-label is--bold">{html.escape(label)}:</td><td class="product--properties-label">{html.escape(value)}</td></tr>'
        for label, value in rows
    )
    return f'<table class="product--properties-table"><tbody>{body}</tbody></table>'


def build_group_meta(skus: list[str], sku_attrs: dict) -> dict:
    return {
        "color": find_group_attr_value(skus, sku_attrs, ["COLOUR_POWERWEAR", "COLOUR", "COLOR"]).lower(),
        "gender": find_group_attr_value(skus, sku_attrs, ["PW_GENDER", "GENDER"]),
        "collection": find_group_attr_value(skus, sku_attrs, ["PW_KTM_COLL", "KTM_COLLECTION"]),
        "playground": find_group_attr_value(skus, sku_attrs, ["PW_KTM_PLAY", "KTM_PLAYGROUND"]),
    }

def is_bad_value(v: str) -> bool:
    if not v:
        return True
    vv = v.strip()
    if not vv:
        return True
    if vv.isdigit():
        return True
    return False

# -----------------------------------------------------
# Loader
# -----------------------------------------------------
def build_handle(key: str, skus: list[str]) -> str:

    clean_skus = [s.strip() for s in skus if s and s.strip()]
    overrides = load_handle_overrides()

    if not clean_skus:
        fallback = key.replace("$M-", "").strip()
        return fallback or slugify(key.replace("$M-", ""))

    if key in overrides["keys"]:
        return overrides["keys"][key]

    sku_override_handles = {overrides["skus"].get(s) for s in clean_skus if overrides["skus"].get(s)}
    if len(sku_override_handles) == 1:
        return next(iter(sku_override_handles))

    if len(clean_skus) == 1:
        return clean_skus[0]

    # ------------------------------------------------
    # CASE 1: KTM numeric variants
    # ------------------------------------------------

    first = clean_skus[0]

    same_length = all(len(s) == len(first) for s in clean_skus)

    if same_length and len(first) >= 3:

        prefix = first[:-1]

        if all(s[:-1] == prefix for s in clean_skus):
            return (prefix + "X")

    # ------------------------------------------------
    # CASE 2: language codes (EN DE FR etc)
    # ------------------------------------------------

    stems = [strip_language_suffix(s) for s in clean_skus]
    if stems and all(stems) and len(set(stems)) == 1:
        # Keep language bundles grouped under XX family handle.
        return f"{stems[0]}XX"

    # ------------------------------------------------
    # CASE 3: mixed KTM part variants
    # ------------------------------------------------

    prefixes = [re.sub(r"[A-Z0-9]+$", "", s) for s in clean_skus]

    if len(set(prefixes)) == 1 and prefixes[0]:
        return prefixes[0]

    # ------------------------------------------------
    # fallback
    # ------------------------------------------------

    fallback = key.replace("$M-", "").strip()
    return fallback or slugify(key.replace("$M-", ""))


def get_attr_value(attrs: dict, include_keywords: list[str], exclude_keywords: list[str] | None = None) -> str:
    if not attrs:
        return ""

    exclude_keywords = exclude_keywords or []

    for aname, val in attrs.items():
        key = (aname or "").lower()
        if not all(k in key for k in include_keywords):
            continue
        if any(k in key for k in exclude_keywords):
            continue
        if val and val.strip():
            return val.strip()

    return ""


def get_variant_option(attrs: dict):
    if not attrs:
        return "", ""

    val = get_attr_value(attrs, ["shoe", "size"])
    if val:
        return "Shoe size EU", val

    val = get_attr_value(attrs, ["double", "helmet"])
    if val:
        return "Double sizes for helmets", val

    val = get_attr_value(attrs, ["language"])
    if not val:
        val = get_attr_value(attrs, ["sprache"])
    if val:
        return "Content language", val.upper()

    val = get_attr_value(attrs, ["teeth"])
    if not val:
        val = get_attr_value(attrs, ["zahn"])
    if val:
        if val.isdigit():
            return "Number of teeth", f"{val} teeth"
        return "Number of teeth", val

    val = get_attr_value(attrs, ["size"], ["shoe", "double"])
    if val:
        return "Size", val.upper() if SIZE_RE.match(val.strip()) else val

    val = get_attr_value(attrs, ["colour"])
    if not val:
        val = get_attr_value(attrs, ["color"])
    if val:
        return "Colour", val

    val = get_attr_value(attrs, ["number"])
    if val:
        return "Number", val

    return "", ""


def parse_weight_grams(raw: str) -> str:
    if not raw:
        return ""

    s = raw.strip().lower().replace(",", ".")
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return ""

    try:
        num = float(m.group(0))
    except ValueError:
        return ""

    if num <= 0:
        return ""

    if "kg" in s:
        grams = int(round(num * 1000))
    elif "g" in s:
        grams = int(round(num))
    elif "lb" in s or "pound" in s:
        grams = int(round(num * 453.59237))
    else:
        # KTM PP/PW fields are generally in kg when no unit is provided.
        grams = int(round(num * 1000))

    return str(grams) if grams > 0 else ""


def get_weight_grams(attrs: dict) -> str:
    if not attrs:
        return ""

    preferred_keys = [
        "PW_WEIGHT",
        "PP_WEIGHT",
        "PW_WEIGHT_SPECIAL",
        "ERP_NETWEIGHT",
        "WEIGHT",
    ]
    for k in preferred_keys:
        if k in attrs:
            grams = parse_weight_grams(attrs[k])
            if grams:
                return grams

    for aname, val in attrs.items():
        key = (aname or "").lower()
        if "weight" in key or "gewicht" in key:
            if "maximum" in key or "max" in key:
                continue
            grams = parse_weight_grams(val)
            if grams:
                return grams

    return ""


def resolve_group_option(skus: list[str], sku_attrs: dict):
    labels = defaultdict(set)
    values_by_sku = {}

    for sku in skus:
        label, value = get_variant_option(sku_attrs.get(sku, {}))
        values_by_sku[sku] = (label, value)
        if label and value:
            labels[label].add(value)

    if not labels:
        return "Title", {sku: "Default Title" for sku in skus}

    label, unique_values = max(labels.items(), key=lambda x: len(x[1]))
    if len(unique_values) < 2:
        return "Title", {sku: "Default Title" for sku in skus}

    option_map = {}
    for sku in skus:
        l, v = values_by_sku.get(sku, ("", ""))
        option_map[sku] = v if l == label and v else "Default Title"

    return label, option_map


def build_hierarchy_titles(structure_index: dict, start_name: str) -> list[str]:
    titles = []
    seen = set()
    current = start_name

    while current and current not in seen:
        seen.add(current)
        node = structure_index.get(current)
        if not node:
            break
        title = (node.get("title") or "").strip()
        if title:
            titles.append(title)
        current = (node.get("parent_name") or "").strip()

    return titles

def load_products():

    print("XML streaming parsen...")

    structure_index = {}
    relations = defaultdict(list)
    sku_attrs = {}
    image_map = defaultdict(list)

    context = etree.iterparse(
        XML_FILE,
        events=("end",),
        tag=("STRUKTUR_ELEMENT", "PRODUKT_ZU_STRUKTUR_ELEMENT", "PRODUKT")
    )

    for event, elem in context:

        tag = elem.tag

        # ---------------- STRUCTURE ----------------
        if tag == "STRUKTUR_ELEMENT":

            name = elem.get("name")

            if name:

                title = (
                    first_text(elem.xpath(
                        f'.//TEXTART[@name="BEZEICHNUNG"]/TEXT[@culture="{CULTURE}"]'
                    ))
                    or name
                )

                description = build_description(elem)
                parent_name = elem.findtext("PARENT_NAME")

                structure_index[name] = {
                    "title": title,
                    "description": description,
                    "parent_name": parent_name
                }

                # images
                if elem.get("ebene") == "MODELL":

                    seen = set()
                    ordered = []

                    for media in elem.findall(".//MEDIENDATEI"):

                        path = (media.text or "").strip()
                        if not path:
                            continue

                        ext = path.lower().rsplit(".", 1)[-1]

                        if f".{ext}" not in IMAGE_EXTENSIONS:
                            continue

                        if path not in seen:
                            seen.add(path)
                            ordered.append(path)

                    image_map[name] = ordered

        # ---------------- RELATIONS ----------------
        elif tag == "PRODUKT_ZU_STRUKTUR_ELEMENT":

            sku = elem.findtext("PRODUKT_NAME")
            key = elem.findtext("ELEMENT_NAME")

            if sku and key:
                relations[key.strip()].append(sku.strip())

        # ---------------- ATTRIBUTES ----------------
        elif tag == "PRODUKT":

            sku = elem.get("name")

            if sku:

                attrs = {}

                for a in elem.findall(".//ATTRIBUTE/ATTRIBUT"):

                    aname = a.get("name")
                    if not aname:
                        continue

                    for aw in a.findall(".//ATTRIBUTWERT"):
                        v = aw.get("name")
                        if not is_bad_value(v):
                            attrs[aname] = v.strip()
                            break

                if attrs:
                    sku_attrs[sku.strip()] = attrs

        elem.clear()

    # -----------------------------------------------------
    # PRODUCTS
    # -----------------------------------------------------

    products = []

    for key, skus in relations.items():

        se = structure_index.get(key)
        if not se:
            continue

        title = se["title"]
        description = se["description"]
        parent_name = se["parent_name"]

        # family handle
        handle = build_handle(key, skus)

        type_value = ""
        category_value = ""

        hierarchy_titles = build_hierarchy_titles(structure_index, parent_name) if parent_name else []
        if hierarchy_titles:
            type_value = hierarchy_titles[0]
        if len(hierarchy_titles) > 1:
            category_value = hierarchy_titles[1]

        if not category_value:
            category_value = type_value

        tags_value = category_value or type_value or ""
        group_meta = build_group_meta(skus, sku_attrs)
        properties_table = build_properties_table_html(skus, sku_attrs)
        if properties_table and properties_table not in description:
            description = f"{description}{properties_table}" if description else properties_table

        images = image_map.get(key, [])

        single = len(skus) == 1
        option_label, option_values = resolve_group_option(skus, sku_attrs)

        for idx, sku in enumerate(skus):

            products.append({
                "handle": handle,
                "sku": sku,
                "title": title if idx == 0 else "",
                "description": description if idx == 0 else "",
                "type": type_value if idx == 0 else "",
                "category": category_value if idx == 0 else "",
                "tags": tags_value if idx == 0 else "",
                "color": group_meta["color"] if idx == 0 else "",
                "gender": group_meta["gender"] if idx == 0 else "",
                "collection": group_meta["collection"] if idx == 0 else "",
                "playground": group_meta["playground"] if idx == 0 else "",
                "variant": "Default Title" if single else option_values.get(sku, sku[-1]),
                "variant_label": option_label,
                "weight_grams": get_weight_grams(sku_attrs.get(sku, {})),
                "images": images
            })

    print(f"{len(products)} producten opgebouwd.")

    return products
