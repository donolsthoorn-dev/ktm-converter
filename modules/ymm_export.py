"""
Build Shopify-style product list + Year/Make/Model fitment rows from KTM XML.

The reference file Product-Ids-*.csv is a Shopify export:
  Created At, Product Id, Product SKU, Product Title, Product Tags
Only Product SKU / Title / Tags can come from XML; Id and Created At need Shopify.

YMM app (e.g. C: Year Make Model Search) typically bulk-assigns rows per product/variant.
Fitment comes from:
- PRODUKT_ZU_STRUKTUR_ELEMENT → Bikes MODELL; and
- inverse lists BEZIEHUNGSTYP ZBH2BIKE on complete-bike PRODUKT (parts inherit that bike's YMM).
"""

from __future__ import annotations

import csv
import io
import os
import re
from collections import defaultdict
from glob import glob

from lxml import etree

from config import CULTURE, IDS_OUTPUT_DIR, XML_FILE, YMM_OUTPUT_DIR, get_active_brand
from modules.shopify_client import get_shopify_products_index, get_shopify_sku_to_product_id
from modules.xml_loader import (
    build_handle,
    build_hierarchy_titles,
    normalize_shopify_product_handle,
)

# Complete motor (ERP) in XML: spare parts linked via ZBH2BIKE lists on the bike PRODUKT.
BIKE_KLASSE = "$KL-ARTICLE_BIKES"
YMM_MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024

# YMM-app Make: alleen motorfietsmerken van de groep (KTM / Husqvarna / GASGAS).
DEFAULT_YMM_OEM_MAKES: frozenset[str] = frozenset({"KTM", "Husqvarna", "GASGAS"})

_YMM_MAKE_CANONICAL: dict[str, str] = {
    "ktm": "KTM",
    "husqvarna": "Husqvarna",
    "hqv": "Husqvarna",
    "hsq": "Husqvarna",
    "gasgas": "GASGAS",
    "gas gas": "GASGAS",
}


def _first_text(nodes):
    for n in nodes:
        if n is not None and n.text and n.text.strip():
            return n.text.strip()
    return ""


def _parse_year(key: str, title: str) -> str:
    tail = key.replace("$M-", "").strip()
    m = re.search(r"(19|20)\d{2}$", tail)
    if m:
        return m.group(0)
    m = re.search(r"\b(19|20)\d{2}\b", title or "")
    if m:
        return m.group(0)
    return ""


def _model_display(title: str, year: str) -> str:
    t = (title or "").strip()
    if year and t.endswith(year):
        t = t[: -len(year)].strip()
    return t or (title or "").strip()


# Motorcycle OEM in YMM Make column (longest match first).
_OEM_MAKE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("royal enfield", "Royal Enfield"),
    ("mv agusta", "MV Agusta"),
    ("husqvarna", "Husqvarna"),
    ("gas gas", "GASGAS"),
    ("gasgas", "GASGAS"),
    ("kawasaki", "Kawasaki"),
    ("yamaha", "Yamaha"),
    ("triumph", "Triumph"),
    ("sherco", "Sherco"),
    ("suzuki", "Suzuki"),
    ("ducati", "Ducati"),
    ("honda", "Honda"),
    ("aprilia", "Aprilia"),
    ("benelli", "Benelli"),
    ("cfmoto", "CFMoto"),
    ("hyosung", "Hyosung"),
    ("piaggio", "Piaggio"),
    ("harley", "Harley-Davidson"),
    ("indian", "Indian"),
    ("beta ", "Beta"),
    ("beta", "Beta"),
    ("bmw", "BMW"),
    ("ktm", "KTM"),
)

_KTM_MODEL_RE = re.compile(
    r"(^|\s)(duke|adventure|super\s*duke|super\s*adventure|"
    r"exc|sx|smc|freeride|enduro|rc\s*\d|rc\d)(\s|$|-)",
    re.I,
)


def _make_from_pseudo_model_key(low: str) -> str:
    key = low.removeprefix("$m-")
    if "kawasaki" in key or key.startswith("zx") or key.startswith("kx"):
        return "Kawasaki"
    if "yamaha" in key or key.startswith("yz"):
        return "Yamaha"
    if "honda" in key or "cbr" in key or "crf" in key or key.startswith("cr"):
        return "Honda"
    if "suzuki" in key or "rmz" in key or "gsx" in key:
        return "Suzuki"
    if "ducati" in key or "panigale" in key:
        return "Ducati"
    if "sherco" in key:
        return "Sherco"
    if "betarr" in key or key.startswith("beta"):
        return "Beta"
    if "husqvarna" in key or "hqv" in key:
        return "Husqvarna"
    if "gasgas" in key or "gg" in key:
        return "GASGAS"
    if "ktm" in key:
        return "KTM"
    return ""


def _make_from_model_title(title: str) -> str:
    """Infer motorcycle OEM from Bikes MODELL title (WP cross-brand fitment)."""
    t = (title or "").strip()
    if not t:
        return ""
    low = t.lower()
    for prefix, make in _OEM_MAKE_PREFIXES:
        if low.startswith(prefix):
            return make
    if low.startswith("beta "):
        return "Beta"
    if low.startswith("$m-"):
        return _make_from_pseudo_model_key(low)
    if re.match(r"^(te|fe|tc|fc|fx)\s", low):
        return "Husqvarna"
    if any(x in low for x in ("vitpilen", "svartpilen", "norden")):
        return "Husqvarna"
    if re.match(r"^701(\s|$)", low) or low.startswith("fs "):
        return "Husqvarna"
    if re.match(r"^(mc|ec|ex|tx|txt|ee|es)\s", low) or low.startswith("mc-"):
        return "GASGAS"
    if _KTM_MODEL_RE.search(low):
        return "KTM"
    if re.match(
        r"^(1[12]\d{2}|[4-9]\d{2}|[1-9]\d?)\s+(sx|xc|rc|duke|adventure|exc|smc|enduro|freeride)",
        low,
    ):
        return "KTM"
    if re.match(r"^(50|65|85|125|150|200|250|300|350|390|450|500|690|790|890|990|1050|1090|1190|1290|1390)\s", low):
        return "KTM"
    return ""


def _detect_make(chain_titles: list[str], chain_keys: list[str]) -> str:
    brand_id = get_active_brand().id
    model_title = (chain_titles[0] if chain_titles else "").strip()
    if model_title:
        from_title = _make_from_model_title(model_title)
        if from_title:
            return from_title
    blob = " ".join(chain_titles).lower() + " " + " ".join(chain_keys).lower()
    if "husqvarna" in blob or "hsq" in blob:
        return "Husqvarna"
    if "gasgas" in blob or "gas gas" in blob:
        return "GASGAS"
    if brand_id == "wp":
        return ""
    if brand_id == "hsq":
        return "Husqvarna"
    if " white power" in blob or blob.startswith("wp ") or " wp " in blob:
        return "WP"
    return "KTM"


def _is_bikes_modell(structure_index: dict, name: str) -> bool:
    """True if this node sits under the catalogue branch whose title is 'Bikes'."""
    cur = name
    seen = set()
    while cur and cur not in seen:
        seen.add(cur)
        node = structure_index.get(cur)
        if not node:
            break
        if (node.get("title") or "").strip() == "Bikes":
            return True
        cur = (node.get("parent_name") or "").strip()
    return False


def _structure_meta(structure_index: dict, start_name: str):
    titles = []
    keys = []
    cur = start_name
    seen = set()
    while cur and cur not in seen:
        seen.add(cur)
        node = structure_index.get(cur)
        if not node:
            break
        keys.append(cur)
        t = (node.get("title") or "").strip()
        if t:
            titles.append(t)
        cur = (node.get("parent_name") or "").strip()
    return titles, keys


def collect_sku_to_ymm_from_structure(
    structure_index: dict, relations: dict
) -> dict[str, set[tuple[str, str, str]]]:
    """
    SKU -> (make, model, year) from PRODUKT_ZU_STRUKTUR_ELEMENT → Bikes MODELL only.
    Shared with Metafields Manager export and ZBH2BIKE merge.
    """
    out: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for key, skus in relations.items():
        se = structure_index.get(key)
        if not se or se.get("ebene") != "MODELL":
            continue
        if not _is_bikes_modell(structure_index, key):
            continue
        title = se["title"]
        year = _parse_year(key, title)
        if not year:
            continue
        chain_titles, chain_keys = _structure_meta(structure_index, key)
        make = _detect_make(chain_titles, chain_keys)
        if not make:
            continue
        model = _model_display(title, year)
        ymm = (make, model, year)
        for sku in skus:
            s = (sku or "").strip()
            if s:
                out[s].add(ymm)
    return dict(out)


def _produkt_is_complete_bike(elem) -> bool:
    """True if this PRODUKT is a motor (KLASSE $KL-ARTICLE_BIKES)."""
    klassen = elem.find("KLASSEN")
    if klassen is None:
        return False
    for k in klassen.findall("KLASSE"):
        if k.get("name") == BIKE_KLASSE:
            return True
    return False


def _first_bezeichnung_any_culture(elem) -> str:
    """Prefer configured CULTURE; many bike PRODUKT only ship BEZEICHNUNG as DE-AT."""
    t = _first_text(elem.xpath(f'.//TEXTART[@name="BEZEICHNUNG"]/TEXT[@culture="{CULTURE}"]'))
    if t:
        return t
    for node in elem.xpath('.//TEXTART[@name="BEZEICHNUNG"]/TEXT'):
        if node is not None and node.text and node.text.strip():
            return node.text.strip()
    return ""


def _ymm_from_bike_produkt_elem(elem) -> set[tuple[str, str, str]]:
    """
    Fallback YMM when the bike SKU is not linked under a Bikes MODELL in the structure tree.
    Uses BEZEICHNUNG (usually contains model name + year).
    """
    bike_sku = (elem.get("name") or "").strip()
    title = _first_bezeichnung_any_culture(elem)
    if not title:
        title = bike_sku
    year = _parse_year(bike_sku, title)
    if not year:
        return set()
    model = _model_display(title, year)
    make = _detect_make([title], [bike_sku])
    if not make:
        return set()
    return {(make, model, year)}


def _produkt_is_nested_beziehungstyp_ref(elem) -> bool:
    """Nested <PRODUKT/> under BEZIEHUNGSTYP — do not clear before the owning PRODUKT ends."""
    p = elem.getparent()
    return p is not None and p.tag == "BEZIEHUNGSTYP"


def stream_zbh2bike_part_ymm(
    xml_file: str,
    structure_sku_ymm: dict[str, set[tuple[str, str, str]]],
) -> dict[str, set[tuple[str, str, str]]]:
    """
    Inverse fitment: on each complete-bike PRODUKT, BEZIEHUNGSTYP ZBH2BIKE lists related part SKUs.
    Each part inherits the bike's YMM (from structure tree if present, else from bike PRODUKT text).

    This captures accessories/kits that only link to a pseudo-$M-SKU in the tree but appear on
    hundreds of bikes via ZBH2BIKE.

    iterparse fires end events for nested PRODUKT refs (e.g. under BEZIEHUNGSTYP) before the
    parent article PRODUKT closes. Clearing inner nodes breaks the parent's tree; skip clear for
    those and clear only standalone article PRODUKT nodes.
    """
    out: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    context = etree.iterparse(xml_file, events=("end",), tag="PRODUKT")
    for _event, elem in context:
        try:
            if not _produkt_is_complete_bike(elem):
                continue
            bike_sku = (elem.get("name") or "").strip()
            if not bike_sku:
                continue
            ymm_bike = set(structure_sku_ymm.get(bike_sku, set()))
            if not ymm_bike:
                ymm_bike = _ymm_from_bike_produkt_elem(elem)
            if not ymm_bike:
                continue
            bez = elem.find("BEZIEHUNGEN")
            if bez is None:
                continue
            for bt in bez.findall("BEZIEHUNGSTYP"):
                if bt.get("name") != "ZBH2BIKE":
                    continue
                for child in bt.findall("PRODUKT"):
                    part = (child.get("name") or "").strip()
                    if not part:
                        continue
                    out[part] |= ymm_bike
        finally:
            if not _produkt_is_nested_beziehungstyp_ref(elem):
                elem.clear()
    return dict(out)


def merge_sku_ymm_maps(
    *maps: dict[str, set[tuple[str, str, str]]],
) -> dict[str, set[tuple[str, str, str]]]:
    """Union of YMM sets per SKU."""
    merged: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for m in maps:
        for sku, tset in m.items():
            merged[sku] |= tset
    return dict(merged)


def build_merged_sku_to_ymm(
    structure_index: dict,
    relations: dict,
    xml_file: str | None = None,
) -> dict[str, set[tuple[str, str, str]]]:
    """Bikes MODELL relations + ZBH2BIKE inverse lists (second XML pass)."""
    path = xml_file or XML_FILE
    struct = collect_sku_to_ymm_from_structure(structure_index, relations)
    zbh = stream_zbh2bike_part_ymm(path, struct)
    return merge_sku_ymm_maps(struct, zbh)


def stream_xml_for_export(xml_path: str | None = None):
    """Single pass: structure + relations (sku -> list of element keys)."""
    path = xml_path or XML_FILE
    structure_index = {}
    relations = defaultdict(list)

    context = etree.iterparse(
        path,
        events=("end",),
        tag=("STRUKTUR_ELEMENT", "PRODUKT_ZU_STRUKTUR_ELEMENT"),
    )
    for _event, elem in context:
        tag = elem.tag
        if tag == "STRUKTUR_ELEMENT":
            name = elem.get("name")
            if name:
                title = (
                    _first_text(
                        elem.xpath(f'.//TEXTART[@name="BEZEICHNUNG"]/TEXT[@culture="{CULTURE}"]')
                    )
                    or name
                )
                structure_index[name] = {
                    "title": title,
                    "parent_name": elem.findtext("PARENT_NAME"),
                    "ebene": elem.get("ebene"),
                }
        elif tag == "PRODUKT_ZU_STRUKTUR_ELEMENT":
            sku = elem.findtext("PRODUKT_NAME")
            key = elem.findtext("ELEMENT_NAME")
            if sku and key:
                relations[key.strip()].append(sku.strip())
        elem.clear()

    return structure_index, relations


def build_product_rows(structure_index: dict, relations: dict):
    """Same grouping as xml_loader.load_products (handle, title, tags, skus)."""
    rows_out = []

    for key, skus in relations.items():
        se = structure_index.get(key)
        if not se:
            continue
        title = se["title"]
        parent_name = se["parent_name"]
        hierarchy_titles = (
            build_hierarchy_titles(structure_index, parent_name) if parent_name else []
        )
        type_value = hierarchy_titles[0] if hierarchy_titles else ""
        category_value = hierarchy_titles[1] if len(hierarchy_titles) > 1 else type_value
        if not category_value:
            category_value = type_value
        tags_value = category_value or type_value or ""

        handle = build_handle(key, skus)

        for idx, sku in enumerate(skus):
            rows_out.append(
                {
                    "handle": handle,
                    "sku": sku,
                    "title": title if idx == 0 else "",
                    "tags": tags_value if idx == 0 else "",
                }
            )
    return rows_out


def export_product_ids_template(path: str, product_rows: list[dict]) -> None:
    """Match Product-Ids-*.csv columns; leave Shopify-only fields empty."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    header = [
        "Created At",
        "Product Id",
        "Product SKU",
        "Product Title",
        "Product Tags",
    ]
    by_handle: dict[str, dict] = {}
    for p in product_rows:
        h = p["handle"]
        cur = by_handle.setdefault(h, {"title": p.get("title") or "", "tags": p.get("tags") or ""})
        if len(p.get("title") or "") > len(cur["title"]):
            cur["title"] = p.get("title") or ""
        if len(p.get("tags") or "") > len(cur["tags"]):
            cur["tags"] = p.get("tags") or ""
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        for h in sorted(by_handle.keys()):
            row = by_handle[h]
            w.writerow(["", "", h, row.get("title") or "", row.get("tags") or ""])


def load_product_ids_from_csv(path: str) -> dict:
    index = {}
    if not path or not os.path.exists(path):
        return index
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sku = normalize_shopify_product_handle(row.get("Product SKU") or "")
            if not sku:
                continue
            index[sku] = {
                "id": (row.get("Product Id") or "").replace("~", "").strip(),
                "created_at": (row.get("Created At") or "").strip(),
                "title": (row.get("Product Title") or "").strip(),
                "tags": (row.get("Product Tags") or "").strip(),
            }
    return index


def find_latest_product_ids_csv() -> str:
    candidates = sorted(glob(os.path.join("input", "Product-Ids-*.csv")))
    return candidates[-1] if candidates else ""


def _lookup_product_id_by_variant_sku(sku: str, sku_to_product_id: dict[str, str]) -> str:
    """
    Shopify stores variant SKU with stable casing; XML / handles may differ in case.
    """
    if not sku or not sku_to_product_id:
        return ""
    for key in (sku, sku.upper(), sku.lower()):
        pid = sku_to_product_id.get(key)
        if pid:
            return pid
    return ""


def export_product_ids_with_shopify_data(
    path: str,
    product_rows: list[dict],
    shopify_index: dict | None,
    fallback_csv_path: str,
    sku_to_shopify_product_id: dict[str, str] | None = None,
) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    header = [
        "Created At",
        "Product Id",
        "Product SKU",
        "Product Title",
        "Product Tags",
    ]
    by_handle: dict[str, dict] = {}
    for p in product_rows:
        h = p["handle"]
        cur = by_handle.setdefault(h, {"title": p.get("title") or "", "tags": p.get("tags") or ""})
        if len(p.get("title") or "") > len(cur["title"]):
            cur["title"] = p.get("title") or ""
        if len(p.get("tags") or "") > len(cur["tags"]):
            cur["tags"] = p.get("tags") or ""

    handle_to_skus: dict[str, list[str]] = defaultdict(list)
    for p in product_rows:
        hs = (p.get("handle") or "").strip()
        sku = (p.get("sku") or "").strip()
        if hs and sku and sku not in handle_to_skus[hs]:
            handle_to_skus[hs].append(sku)

    fallback_index = load_product_ids_from_csv(fallback_csv_path)
    sku_to_shopify_product_id = sku_to_shopify_product_id or {}

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        for h in sorted(by_handle.keys()):
            xml_row = by_handle[h]
            api = (shopify_index or {}).get(h, {})
            fb = fallback_index.get(h, {})
            created_at = api.get("created_at") or fb.get("created_at") or ""
            product_id = api.get("id") or fb.get("id") or ""
            # Shopify product index is keyed by URL handle (slug), not ERP article number.
            # When the XML handle equals the variant SKU, match via variants API map.
            if not product_id and sku_to_shopify_product_id:
                for sku in handle_to_skus.get(h, []):
                    product_id = _lookup_product_id_by_variant_sku(sku, sku_to_shopify_product_id)
                    if product_id:
                        break
                if not product_id:
                    product_id = _lookup_product_id_by_variant_sku(h, sku_to_shopify_product_id)
            title = api.get("title") or xml_row.get("title") or fb.get("title") or ""
            tags = api.get("tags") or xml_row.get("tags") or fb.get("tags") or ""
            w.writerow([created_at, product_id, h, title, tags])


def _build_sku_to_keys(relations: dict) -> dict[str, list[str]]:
    m: dict[str, list[str]] = defaultdict(list)
    for k, sks in relations.items():
        for s in sks:
            if s:
                m[s].append(k)
    return m


def resolve_handle_for_sku(
    sku: str, relations: dict, sku_to_keys: dict[str, list[str]] | None = None
) -> str:
    """
    Map a variant SKU to its Shopify-style product handle.
    A SKU may appear under a bike MODELL key (fitment) and under its own product key;
    prefer the relation group that represents the sellable product (usually single-SKU).
    """
    sku = (sku or "").strip()
    if not sku:
        return ""
    candidates = (sku_to_keys or {}).get(sku) or [k for k, sks in relations.items() if sku in sks]
    if not candidates:
        return sku

    def score_key(k: str) -> tuple:
        sks = relations[k]
        h = build_handle(k, sks)
        return (
            1 if len(sks) == 1 and sks[0] == sku else 0,
            1 if h == sku else 0,
            1 if h.lower() == sku.lower() else 0,
            -len(sks),
            -len(k),
            k,
        )

    best_k = max(candidates, key=score_key)
    return build_handle(best_k, relations[best_k])


def build_sku_to_candidate_handles(product_rows: list[dict]) -> dict[str, list[str]]:
    """
    Each variant SKU can appear under multiple STRUKTUR_ELEMENT keys in the XML, each
    with a different computed handle. Shopify Product Id is keyed by one of those
    handles in product_ids_from_xml.csv — not always the same one resolve_handle_for_sku picks.
    """
    m: dict[str, list[str]] = defaultdict(list)
    for p in product_rows:
        sku = (p.get("sku") or "").strip()
        h = (p.get("handle") or "").strip()
        if not sku or not h:
            continue
        if h not in m[sku]:
            m[sku].append(h)
    return dict(m)


def _product_id_for_sku(
    sku: str,
    resolved_handle: str,
    candidate_handles: list[str],
    handle_to_product_id: dict[str, str],
) -> str:
    """Pick Shopify product id: prefer resolved handle, then any other XML handle with an id."""
    seen: set[str] = set()
    all_h: list[str] = []
    for h in (resolved_handle, *candidate_handles):
        if h and h not in seen:
            seen.add(h)
            all_h.append(h)

    def sort_key(h: str) -> tuple:
        pid = handle_to_product_id.get(h, "")
        return (
            1 if pid else 0,
            1 if h == sku else 0,
            1 if h == resolved_handle else 0,
            -len(h),
            h,
        )

    for h in sorted(all_h, key=sort_key, reverse=True):
        pid = handle_to_product_id.get(h, "")
        if pid:
            return pid
    return ""


def export_ymm_fitment(
    path: str,
    structure_index: dict,
    relations: dict,
    handle_to_product_id: dict[str, str] | None = None,
    product_rows: list[dict] | None = None,
    sku_to_shopify_product_id: dict[str, str] | None = None,
    xml_file: str | None = None,
    filter_handles: set[str] | None = None,
    filter_makes: set[str] | None = None,
    sku_to_ymm: dict[str, set[tuple[str, str, str]]] | None = None,
) -> int:
    """
    Full YMM rows for app bulk insert template:
      Product Ids, Make, Model, Year

    Sources:
    - PRODUKT_ZU_STRUKTUR_ELEMENT → Bikes MODELL (unchanged semantics, union)
    - BEZIEHUNGSTYP ZBH2BIKE on complete-bike PRODUKT: parts inherit that bike's YMM

    For each variant SKU with at least one YMM tuple, resolve the real product handle,
    then emit (Product Id, Make, Model, Year). Rows deduped by
    (resolved handle, make, model, year).
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    count = 0
    header = ["Product Ids", "Make", "Model", "Year"]
    seen_rows = set()
    handle_to_product_id = handle_to_product_id or {}

    if sku_to_ymm is None:
        sku_to_ymm = build_merged_sku_to_ymm(
            structure_index, relations, xml_file=xml_file or XML_FILE
        )
    from modules.cross_brand_ymm import (
        build_normalized_sku_ymm_lookup,
        ymm_lookup_for_sku,
    )

    ymm_lookup = build_normalized_sku_ymm_lookup(sku_to_ymm)

    brand_skus = {s for sks in relations.values() for s in sks if s}
    if product_rows:
        brand_skus |= {
            (p.get("sku") or "").strip()
            for p in product_rows
            if (p.get("sku") or "").strip()
        }
    sku_to_keys = _build_sku_to_keys(relations)
    sku_to_handle = {
        s: resolve_handle_for_sku(s, relations, sku_to_keys) for s in brand_skus
    }
    sku_to_candidate_handles = build_sku_to_candidate_handles(product_rows) if product_rows else {}
    sku_to_shopify_product_id = sku_to_shopify_product_id or {}

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for sku in sorted(brand_skus, key=lambda x: x.upper()):
            ymm_set = ymm_lookup_for_sku(ymm_lookup, sku)
            if not ymm_set:
                continue
            handle = sku_to_handle.get(sku, sku)
            product_id = _product_id_for_sku(
                sku,
                handle,
                sku_to_candidate_handles.get(sku, []),
                handle_to_product_id,
            )
            if not product_id:
                product_id = _lookup_product_id_by_variant_sku(sku, sku_to_shopify_product_id)
            if filter_handles is not None and handle not in filter_handles:
                continue
            for make, model, year in sorted(ymm_set, key=lambda t: (t[0], t[1], t[2])):
                if filter_makes is not None and make not in filter_makes:
                    continue
                sig = (handle, make, model, year)
                if sig in seen_rows:
                    continue
                seen_rows.add(sig)
                w.writerow([product_id, make, model, year])
                count += 1
    return count


def _csv_row_size_bytes(row: list[str]) -> int:
    """Approx UTF-8 byte size of a CSV row as written by csv.writer."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(row)
    return len(buf.getvalue().encode("utf-8"))


def split_csv_max_bytes_with_header(
    path: str, max_bytes: int = YMM_MAX_FILE_SIZE_BYTES
) -> list[str]:
    """
    Split CSV into chunks <= max_bytes, each with header row.
    Returns list of output paths. If file is already small enough, returns [path].
    """
    if not os.path.exists(path):
        return []
    if os.path.getsize(path) <= max_bytes:
        return [path]

    base, ext = os.path.splitext(path)
    out_paths: list[str] = []

    with open(path, encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        header = next(reader, None)
        if not header:
            return [path]

        header_size = _csv_row_size_bytes(header)
        part_idx = 1
        cur_path = f"{base}_part_{part_idx:03d}{ext}"
        cur_file = open(cur_path, "w", encoding="utf-8", newline="")
        cur_writer = csv.writer(cur_file)
        cur_writer.writerow(header)
        cur_size = header_size
        rows_in_part = 0
        out_paths.append(cur_path)

        try:
            for row in reader:
                row_size = _csv_row_size_bytes(row)
                if rows_in_part > 0 and (cur_size + row_size) > max_bytes:
                    cur_file.close()
                    part_idx += 1
                    cur_path = f"{base}_part_{part_idx:03d}{ext}"
                    cur_file = open(cur_path, "w", encoding="utf-8", newline="")
                    cur_writer = csv.writer(cur_file)
                    cur_writer.writerow(header)
                    cur_size = header_size
                    rows_in_part = 0
                    out_paths.append(cur_path)

                cur_writer.writerow(row)
                cur_size += row_size
                rows_in_part += 1
        finally:
            cur_file.close()

    os.remove(path)
    return out_paths


def _ensure_ymm_all_named_as_parts(ymm_path: str, split_paths: list[str]) -> list[str]:
    """
    Na splitsing: volledige ALL-export gebruikt altijd ymm_APP_import_ALL_part_NNN.csv
    (ook één deel onder de 10MB-grens), zodat upload-stappen niet afhangen van
    een losse ymm_APP_import_ALL.csv.
    """
    if not split_paths or len(split_paths) != 1:
        return split_paths
    only = split_paths[0]
    base = os.path.basename(only)
    if base != "ymm_APP_import_ALL.csv":
        return split_paths
    d = os.path.dirname(only) or "."
    target = os.path.join(d, "ymm_APP_import_ALL_part_001.csv")
    if os.path.abspath(only) != os.path.abspath(target):
        os.replace(only, target)
    return [target]


def build_handle_to_product_id(product_ids_path: str) -> dict[str, str]:
    out = {}
    if not product_ids_path or not os.path.exists(product_ids_path):
        return out
    with open(product_ids_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            handle = normalize_shopify_product_handle(row.get("Product SKU") or "")
            pid = (row.get("Product Id") or "").replace("~", "").strip()
            if handle and pid:
                out[handle] = pid
    return out


def enrich_handle_to_product_id_from_shopify(
    handle_to_product_id: dict[str, str],
    handles: set[str],
    *,
    force_refresh: bool = False,
) -> int:
    """Vul ontbrekende handle→Product Id aan vanuit Shopify productindex (cache/API)."""
    if not handles:
        return 0
    try:
        index = get_shopify_products_index(force_refresh=force_refresh)
    except Exception as e:
        print(f"Shopify index niet geladen voor Product Id-aanvulling: {e}", flush=True)
        return 0
    added = 0
    for h in handles:
        if handle_to_product_id.get(h):
            continue
        ent = index.get(h) or index.get(h.lower()) or {}
        pid = (ent.get("id") or "").replace("~", "").strip()
        if pid:
            handle_to_product_id[h] = pid
            added += 1
    return added


def _canonicalize_make_filter_value(value: str) -> str:
    key = (value or "").strip().lower().replace("_", " ")
    return _YMM_MAKE_CANONICAL.get(key, (value or "").strip())


def resolve_ymm_make_filter(
    raw: list[str] | None = None,
    *,
    all_makes: bool = False,
) -> set[str] | None:
    """
    Standaard: alleen KTM + Husqvarna + GASGAS.
    all_makes=True of expliciet lege lijst via caller: geen Make-filter.
    """
    if all_makes:
        return None
    if raw:
        out: set[str] = set()
        for item in raw:
            for part in item.split(","):
                p = part.strip()
                if p:
                    out.add(_canonicalize_make_filter_value(p))
        return out or None
    return set(DEFAULT_YMM_OEM_MAKES)


def run_exports(
    product_ids_path: str | None = None,
    ymm_path: str | None = None,
    filter_handles: set[str] | None = None,
    filter_makes: set[str] | None = None,
    *,
    ymm_all_makes: bool = False,
    unified_cross_brand_ymm: bool = True,
) -> tuple[str, str, int]:
    if ymm_all_makes:
        effective_filter_makes: set[str] | None = None
    elif filter_makes is not None:
        effective_filter_makes = filter_makes
    else:
        effective_filter_makes = set(DEFAULT_YMM_OEM_MAKES)

    print("XML inlezen (kan even duren, geen output tot dit klaar is)...", flush=True)
    structure_index, relations = stream_xml_for_export()
    print(
        f"XML klaar: {len(structure_index)} structuur-nodes, {len(relations)} koppelingen.",
        flush=True,
    )
    product_rows = build_product_rows(structure_index, relations)
    if filter_handles is not None:
        if len(filter_handles) == 0:
            raise ValueError("filter_handles is leeg")
        before = len(product_rows)
        product_rows = [
            p for p in product_rows if (p.get("handle") or "").strip() in filter_handles
        ]
        print(
            f"Delta filter: {before} → {len(product_rows)} productregels "
            f"({len(filter_handles)} handles in filter).",
            flush=True,
        )

    if product_ids_path is None:
        product_ids_path = os.path.join(
            IDS_OUTPUT_DIR,
            "product_ids_from_xml_delta.csv" if filter_handles else "product_ids_from_xml.csv",
        )
    if ymm_path is None:
        make_suffix = ""
        if effective_filter_makes and effective_filter_makes != set(DEFAULT_YMM_OEM_MAKES):
            slug = "_".join(sorted(m.replace(" ", "") for m in effective_filter_makes))
            make_suffix = f"_{slug}"
        ymm_path = os.path.join(
            YMM_OUTPUT_DIR,
            "ymm_APP_import_DELTA.csv"
            if filter_handles
            else f"ymm_APP_import_ALL{make_suffix}.csv",
        )
    shopify_index = None
    sku_to_shopify_product_id: dict[str, str] = {}
    fallback_csv = find_latest_product_ids_csv()
    try:
        shopify_index = get_shopify_products_index()
        print(f"Shopify productindex geladen: {len(shopify_index)} handles", flush=True)
    except Exception as e:
        print(f"Shopify productindex API niet bereikbaar, fallback CSV gebruiken: {e}")

    try:
        sku_to_shopify_product_id = get_shopify_sku_to_product_id()
        print(
            f"Shopify variant SKU→Product Id: {len(sku_to_shopify_product_id)} SKU's",
            flush=True,
        )
    except Exception as e:
        print(f"Shopify SKU→Product Id niet geladen (YMM mist dan vaak Id's): {e}", flush=True)

    export_product_ids_with_shopify_data(
        product_ids_path,
        product_rows,
        shopify_index,
        fallback_csv,
        sku_to_shopify_product_id=sku_to_shopify_product_id,
    )
    handle_to_product_id = build_handle_to_product_id(product_ids_path)
    sku_to_ymm: dict[str, set[tuple[str, str, str]]] | None = None
    if unified_cross_brand_ymm:
        from modules.cross_brand_ymm import (
            build_canonical_sku_to_ymm,
            resolve_cross_brand_xml_paths,
        )

        xml_paths = resolve_cross_brand_xml_paths()
        print(
            f"Cross-brand YMM: union uit {len(xml_paths)} XML's voor fitment-rijen…",
            flush=True,
        )
        sku_to_ymm = build_canonical_sku_to_ymm(
            xml_paths,
            filter_makes=effective_filter_makes,
        )
    else:
        print(
            "Tweede XML-pass (ZBH2BIKE: motor → onderdelen) — alleen huidig merk-XML…",
            flush=True,
        )
    n_ymm = export_ymm_fitment(
        ymm_path,
        structure_index,
        relations,
        handle_to_product_id=handle_to_product_id,
        product_rows=product_rows,
        sku_to_shopify_product_id=sku_to_shopify_product_id,
        filter_handles=filter_handles,
        filter_makes=effective_filter_makes,
        sku_to_ymm=sku_to_ymm,
    )
    if effective_filter_makes:
        print(
            f"YMM Make-filter: {', '.join(sorted(effective_filter_makes))}",
            flush=True,
        )
    # Max 10MB per bestand alleen voor volledige catalogus (ALL), niet voor delta-export:
    # dan blijft ymm_APP_import_DELTA.csv één bestand (of handmatig splitsen indien nodig).
    if filter_handles is None:
        ymm_files = split_csv_max_bytes_with_header(ymm_path, max_bytes=YMM_MAX_FILE_SIZE_BYTES)
        if not ymm_files:
            ymm_files = [ymm_path]
        ymm_files = _ensure_ymm_all_named_as_parts(ymm_path, ymm_files)
        if len(ymm_files) > 1:
            print(
                f"YMM CSV (volledige catalogus) gesplitst in {len(ymm_files)} delen "
                f"(max 10 MB per deel): {ymm_files[0]} … {ymm_files[-1]}",
                flush=True,
            )
        else:
            print(
                f"YMM CSV (volledige catalogus): {ymm_files[0]} "
                f"(max {YMM_MAX_FILE_SIZE_BYTES // (1024 * 1024)} MB per deel indien gesplitst).",
                flush=True,
            )
        return product_ids_path, ymm_files[0], n_ymm
    return product_ids_path, ymm_path, n_ymm
