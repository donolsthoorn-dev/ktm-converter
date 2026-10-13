"""
Year/Make/Model fitment uit CBEXPDN-XML.

Fitment komt uit:
- PRODUKT_ZU_STRUKTUR_ELEMENT → Bikes MODELL; and
- inverse lists BEZIEHUNGSTYP ZBH2BIKE on complete-bike PRODUKT (parts inherit that bike's YMM).

Gebruikt door de metafields-export en de canonical Supabase-pipeline (fits_on / ymm_summary).
"""

from __future__ import annotations

import csv
import io
import os
import re
from collections import defaultdict

from lxml import etree

from config import CULTURE, XML_FILE, get_active_brand
from modules.shopify_client import get_shopify_products_index
from modules.pricing_loader import canonical_erp_sku_from_xml, erp_sku_keys_for_active_brand
from modules.xml_loader import (
    build_handle,
    build_hierarchy_titles,
    normalize_shopify_product_handle,
)

# Complete motor (ERP) in XML: spare parts linked via ZBH2BIKE lists on the bike PRODUKT.
BIKE_KLASSE = "$KL-ARTICLE_BIKES"
YMM_MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024

# OEM-merken voor fits_on: KTM / Husqvarna / GASGAS.
DEFAULT_YMM_OEM_MAKES: frozenset[str] = frozenset({"KTM", "Husqvarna", "GASGAS"})

_YMM_MAKE_CANONICAL: dict[str, str] = {
    "ktm": "KTM",
    "husqvarna": "Husqvarna",
    "hqv": "Husqvarna",
    "hsq": "Husqvarna",
    "gasgas": "GASGAS",
    "gas gas": "GASGAS",
}

_ymm_erp_sku_keys_cache: set[str] | None = None
_ymm_erp_sku_keys_resolved = False


def _normalize_xml_sku_for_ymm(raw: str) -> str:
    """Zelfde WP -00→ERP-regel als xml_loader.load_products."""
    global _ymm_erp_sku_keys_cache, _ymm_erp_sku_keys_resolved
    if not _ymm_erp_sku_keys_resolved:
        _ymm_erp_sku_keys_cache = erp_sku_keys_for_active_brand()
        _ymm_erp_sku_keys_resolved = True
    return canonical_erp_sku_from_xml(raw, erp_sku_keys=_ymm_erp_sku_keys_cache)


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
                    part_raw = (child.get("name") or "").strip()
                    if not part_raw:
                        continue
                    part = _normalize_xml_sku_for_ymm(part_raw)
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
                relations[key.strip()].append(_normalize_xml_sku_for_ymm(sku))
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


