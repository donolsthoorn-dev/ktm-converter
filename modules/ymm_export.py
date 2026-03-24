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
from glob import glob
from collections import defaultdict

from lxml import etree

from config import CULTURE, XML_FILE, REPORT_OUTPUT_DIR
from modules.shopify_client import get_shopify_products_index, get_shopify_sku_to_product_id
from modules.xml_loader import build_handle, build_hierarchy_titles

# Complete motor (ERP) in XML: spare parts linked via ZBH2BIKE lists on the bike PRODUKT.
BIKE_KLASSE = "$KL-ARTICLE_BIKES"
YMM_MAX_FILE_SIZE_BYTES = 19 * 1024 * 1024


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


def _detect_make(chain_titles: list[str], chain_keys: list[str]) -> str:
    blob = " ".join(chain_titles).lower() + " " + " ".join(chain_keys).lower()
    if "husqvarna" in blob or "hsq" in blob:
        return "Husqvarna"
    if "gasgas" in blob or "gas gas" in blob:
        return "GASGAS"
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
    t = _first_text(
        elem.xpath(f'.//TEXTART[@name="BEZEICHNUNG"]/TEXT[@culture="{CULTURE}"]')
    )
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


def stream_xml_for_export():
    """Single pass: structure + relations (sku -> list of element keys)."""
    structure_index = {}
    relations = defaultdict(list)

    context = etree.iterparse(
        XML_FILE,
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
                        elem.xpath(
                            f'.//TEXTART[@name="BEZEICHNUNG"]/TEXT[@culture="{CULTURE}"]'
                        )
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
        cur = by_handle.setdefault(
            h, {"title": p.get("title") or "", "tags": p.get("tags") or ""}
        )
        if len((p.get("title") or "")) > len(cur["title"]):
            cur["title"] = p.get("title") or ""
        if len((p.get("tags") or "")) > len(cur["tags"]):
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
            sku = (row.get("Product SKU") or "").strip()
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


def _lookup_product_id_by_variant_sku(
    sku: str, sku_to_product_id: dict[str, str]
) -> str:
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
        cur = by_handle.setdefault(
            h, {"title": p.get("title") or "", "tags": p.get("tags") or ""}
        )
        if len((p.get("title") or "")) > len(cur["title"]):
            cur["title"] = p.get("title") or ""
        if len((p.get("tags") or "")) > len(cur["tags"]):
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
                    product_id = _lookup_product_id_by_variant_sku(
                        sku, sku_to_shopify_product_id
                    )
                    if product_id:
                        break
                if not product_id:
                    product_id = _lookup_product_id_by_variant_sku(
                        h, sku_to_shopify_product_id
                    )
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


def resolve_handle_for_sku(sku: str, relations: dict, sku_to_keys: dict[str, list[str]] | None = None) -> str:
    """
    Map a variant SKU to its Shopify-style product handle.
    A SKU may appear under a bike MODELL key (fitment) and under its own product key;
    prefer the relation group that represents the sellable product (usually single-SKU).
    """
    sku = (sku or "").strip()
    if not sku:
        return ""
    candidates = (sku_to_keys or {}).get(sku) or [
        k for k, sks in relations.items() if sku in sks
    ]
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

    sku_to_ymm = build_merged_sku_to_ymm(
        structure_index, relations, xml_file=xml_file or XML_FILE
    )

    all_skus = {s for sks in relations.values() for s in sks if s} | set(sku_to_ymm.keys())
    sku_to_keys = _build_sku_to_keys(relations)
    sku_to_handle = {
        s: resolve_handle_for_sku(s, relations, sku_to_keys) for s in all_skus
    }
    sku_to_candidate_handles = (
        build_sku_to_candidate_handles(product_rows) if product_rows else {}
    )
    sku_to_shopify_product_id = sku_to_shopify_product_id or {}

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for sku, ymm_set in sorted(sku_to_ymm.items(), key=lambda x: x[0]):
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
                product_id = _lookup_product_id_by_variant_sku(
                    sku, sku_to_shopify_product_id
                )
            for make, model, year in sorted(ymm_set, key=lambda t: (t[0], t[1], t[2])):
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


def split_csv_max_bytes_with_header(path: str, max_bytes: int = YMM_MAX_FILE_SIZE_BYTES) -> list[str]:
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

    with open(path, "r", encoding="utf-8", newline="") as src:
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
            handle = (row.get("Product SKU") or "").strip()
            pid = (row.get("Product Id") or "").replace("~", "").strip()
            if handle and pid:
                out[handle] = pid
    return out


def run_exports(
    product_ids_path: str | None = None,
    ymm_path: str | None = None,
) -> tuple[str, str, int]:
    print("XML inlezen (kan even duren, geen output tot dit klaar is)...", flush=True)
    structure_index, relations = stream_xml_for_export()
    print(
        f"XML klaar: {len(structure_index)} structuur-nodes, {len(relations)} koppelingen.",
        flush=True,
    )
    product_rows = build_product_rows(structure_index, relations)
    product_ids_path = product_ids_path or os.path.join(
        REPORT_OUTPUT_DIR, "product_ids_from_xml.csv"
    )
    ymm_path = ymm_path or os.path.join(REPORT_OUTPUT_DIR, "ymm_APP_import_ALL.csv")
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
    print(
        "Tweede XML-pass (ZBH2BIKE: motor → onderdelen) voor YMM-export…",
        flush=True,
    )
    n_ymm = export_ymm_fitment(
        ymm_path,
        structure_index,
        relations,
        handle_to_product_id=handle_to_product_id,
        product_rows=product_rows,
        sku_to_shopify_product_id=sku_to_shopify_product_id,
    )
    ymm_files = split_csv_max_bytes_with_header(
        ymm_path, max_bytes=YMM_MAX_FILE_SIZE_BYTES
    )
    if len(ymm_files) > 1:
        print(
            f"YMM CSV gesplitst in {len(ymm_files)} delen (max 19MB): {ymm_files[0]} … {ymm_files[-1]}",
            flush=True,
        )
        return product_ids_path, ymm_files[0], n_ymm
    return product_ids_path, ymm_path, n_ymm
