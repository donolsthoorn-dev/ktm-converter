"""
CSV compatible with Metafields Manager product import (see docs on the app site).

Builds the same column layout as a typical export, e.g.:
  id, handle, title, fits_on, fits_on_year, fits_on_make, fits_on_model,
  fits_on_year_new, fits_on_make_new, fits_on_model_new, ymm_summary,
  parts_*, global_fits_on_*, MPN

`fits_on` is a JSON object: { "MAKE": { "Model name": ["2020","2021"], ... }, ... }
aggregated from KTM XML: Bikes MODELL fitment plus inverse ZBH2BIKE lists on complete bikes.

Export gebruikt voor alle cellen vanaf kolom `fits_on` t/m `MPN` hoofdletters in de inhoud (JSON-keys en -strings, platte kolommen, MPN), conform veel Metafields Manager-exports.

Docs: https://metafieldsmanager.thebestagency.com/docs
"""

from __future__ import annotations

import csv
import json
import os
import re
from collections import defaultdict

from config import REPORT_OUTPUT_DIR, XML_FILE
from modules.ymm_export import (
    build_merged_sku_to_ymm,
    build_product_rows,
    build_handle_to_product_id,
    stream_xml_for_export,
)

def load_shopify_product_merge_csv(path: str | None) -> dict[str, dict[str, str]]:
    """
    Shopify product CSV (export uit admin) met minimaal kolom Handle.
    Optioneel: id, title, fits_on, fits_on_year, … (zelfde namen als METAFIELDS_HEADER,
    hoofdletterongevoelig). Wordt gebruikt om:
    - fits_on te vullen als de SKU niet (meer) in de KTM-XML staat;
    - regels toe te voegen voor producten die alleen in Shopify bestaan.
    """
    if not path or not os.path.exists(path):
        return {}
    out: dict[str, dict[str, str]] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}
        fields = {name.strip().lower(): name for name in reader.fieldnames}
        handle_col = None
        for candidate in ("handle", "product handle"):
            if candidate in fields:
                handle_col = fields[candidate]
                break
        if not handle_col:
            return {}
        col_for: dict[str, str] = {}
        for key in METAFIELDS_HEADER:
            kl = key.lower()
            if kl in fields:
                col_for[key] = fields[kl]
        for row in reader:
            h = (row.get(handle_col) or "").strip()
            if not h:
                continue
            entry = {k: (row.get(col) or "").strip() for k, col in col_for.items()}
            # Shopify-export gebruikt vaak "Fits on" i.p.v. fits_on
            if not entry.get("fits_on"):
                for alt in ("fits on", "fits_on"):
                    if alt in fields:
                        entry["fits_on"] = (row.get(fields[alt]) or "").strip()
                        break
            out[h] = entry
    return out


def _recursive_upper_json_strings(obj):
    """Metafields Manager / Shopify-export gebruikt vaak HOOFDLETTERS in fits_on JSON en platte kolommen."""
    if isinstance(obj, dict):
        return {
            str(k).upper(): _recursive_upper_json_strings(v) for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_recursive_upper_json_strings(x) for x in obj]
    if isinstance(obj, str):
        return obj.upper()
    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return obj


def _upper_fits_on_json_cell(value: str) -> str:
    """fits_on: geldige JSON → alle strings hoofdletters; anders hele cel .upper()."""
    v = (value or "").strip()
    if not v:
        return ""
    try:
        data = json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return v.upper()
    return json.dumps(
        _recursive_upper_json_strings(data),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _upper_plain_metafield_cell(value: str) -> str:
    """Kolommen vanaf fits_on_year t/m MPN: platte tekst in hoofdletters."""
    return (value or "").upper()


METAFIELDS_HEADER = [
    "id",
    "handle",
    "title",
    "fits_on",
    "fits_on_year",
    "fits_on_make",
    "fits_on_model",
    "fits_on_year_new",
    "fits_on_make_new",
    "fits_on_model_new",
    "ymm_summary",
    "parts_compartment",
    "parts_floor",
    "parts_shelve",
    "parts_list",
    "parts_box",
    "parts_row",
    "global_fits_on_make_new",
    "global_fits_on_model_new",
    "global_fits_on_year_new",
    "MPN",
]


def _build_handle_to_skus(product_rows: list[dict]) -> dict[str, list[str]]:
    m: dict[str, list[str]] = defaultdict(list)
    for p in product_rows:
        h = (p.get("handle") or "").strip()
        sku = (p.get("sku") or "").strip()
        if h and sku and sku not in m[h]:
            m[h].append(sku)
    return dict(m)


def _build_handle_to_title(product_rows: list[dict]) -> dict[str, str]:
    by_h: dict[str, str] = {}
    for p in product_rows:
        h = (p.get("handle") or "").strip()
        t = (p.get("title") or "").strip()
        if not h:
            continue
        if len(t) > len(by_h.get(h, "")):
            by_h[h] = t
    return by_h


def _ymm_tuples_to_fits_on_json(tuples: set[tuple[str, str, str]]) -> str:
    """Nested dict MAKE -> model -> [years] as JSON (Metafields Manager: inhoud in HOOFDLETTERS)."""
    nested: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for make, model, year in tuples:
        nested[make.upper()][model.upper()].add((year or "").upper())
    out: dict[str, dict[str, list[str]]] = {}
    for make in sorted(nested.keys()):
        out[make] = {}
        for model in sorted(nested[make].keys()):
            out[make][model] = sorted(
                nested[make][model], key=lambda y: (len(y), y)
            )
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


def _pipe_join_sorted(values: set[str]) -> str:
    return "||".join(sorted((v or "").upper() for v in values))


# Volgorde tussen haakjes — afgestemd op Metafields-handexports: EXC, SX, XC, daarna XCF (XC-F 4T apart van 2T-XC)
_LINE_TAG_ORDER = ("EXC", "SX", "XC", "XCF", "ENDURO", "STREET")


def _extract_displacement_cc(model: str) -> int | None:
    """
    KTM/Husqvarna-achtige modelnamen: '125 SX', '450 SX-F Factory', soms '2026 KTM 250 SX-F …'.
    Geeft motorinhoud (cc) of None.
    """
    s = (model or "").strip()
    if not s:
        return None
    # Jaar vooraan (XML-fouten): "2026 KTM 250 SX-F …"
    s = re.sub(r"^(20\d{2})\s+", "", s)
    s = re.sub(r"^KTM\s+", "", s, flags=re.I)
    s = s.strip()
    m = re.match(r"^(\d{2,4})\s+", s)
    if m:
        n = int(m.group(1))
        if 50 <= n <= 2000:
            return n
    # Laatste redmiddel: eerste plausibele cc vóór SX/XC/EXC/DUKE
    m = re.search(
        r"\b(\d{2,4})\s+(?:SX|XC|EXC|DUKE|SUPER|SMC|ADVENTURE)",
        s,
        re.I,
    )
    if m:
        n = int(m.group(1))
        if 50 <= n <= 2000:
            return n
    return None


def _classify_model_line_tag(model: str) -> str | None:
    """
    Modellijn voor tussenhaakjes (handmatige Metafields-stijl):
    - SX: motocross (SX, SX-F)
    - XC: 2T cross-country / XC-W (zonder XC-F — dat is XCF)
    - XCF: cross-country 4T (modelnaam bevat XC-F)
    - EXC: enduro (EXC, EXC-F, EXC TPI, …)
    - STREET: Duke / Adventure / SMC
    """
    u = (model or "").upper()
    if not u.strip():
        return None
    if re.search(r"\bDUKE\b|\bSUPER\s+DUKE\b|\bADVENTURE\b|\bSMC\b", u):
        return "STREET"
    # XC-F = eigen lijn (XCF). Let op: "EXC-F" bevat als substring "XC-F" — daarom alleen echt
    # cc + spatie + XC-F (KTM-notatie), niet blind 'XC-F in string'.
    if re.search(r"\d{2,4}\s+XC-F\b", u):
        return "XCF"
    # Enduro EXC (EXC-F hoort bij EXC, niet bij XCF)
    if (
        re.search(r"\bEXC\b", u)
        or "EXC-F" in u
        or "EXC-W" in u
        or "EXC TPI" in u
        or "EXC SIX" in u
        or "EXC CKD" in u
    ):
        return "EXC"
    # Motocross
    if "SX-F" in u or re.search(r"\d+\s+SX\b", u):
        return "SX"
    # 2T XC / XC-W / '… XC' zonder F (niet SX-F: geen SX in eerste token na cc)
    if (
        "XC-W" in u
        or "XC TPI" in u
        or re.search(r"\d+\s+XC\b", u)
    ):
        return "XC"
    if re.search(r"\bENDURO\b", u):
        return "ENDURO"
    return None


def _sort_line_tags(tags: set[str]) -> list[str]:
    return [t for t in _LINE_TAG_ORDER if t in tags]


def _ymm_summary(tuples: set[tuple[str, str, str]]) -> str:
    """
    Rijke samenvatting zoals veel Metafields-exports:
      KTM 125-500 (EXC, SX, XC, XCF) 2019-2023
      (XCF = modellen met «cc XC-F», los van 2T-XC en van EXC/EXC-F.)
    Fallback als cc/lijn niet te parsen valt:
      KTM — 2023-2026
    """
    if not tuples:
        return ""
    years: list[int] = []
    for t in tuples:
        y = t[2]
        if y.isdigit():
            years.append(int(y))
    y_lo = min(years) if years else None
    y_hi = max(years) if years else None
    y_part = f"{y_lo}-{y_hi}" if years and y_lo is not None else ""

    makes_upper = {t[0].upper() for t in tuples}
    make_part = ", ".join(sorted(makes_upper))

    ccs: list[int] = []
    line_tags: set[str] = set()
    for _make, model, _y in tuples:
        cc = _extract_displacement_cc(model)
        if cc is not None:
            ccs.append(cc)
        tag = _classify_model_line_tag(model)
        if tag:
            line_tags.add(tag)

    # Eén merk + cc-range + minstens één lijn-tag → volledige zin
    if (
        len(makes_upper) == 1
        and ccs
        and line_tags
        and y_part
    ):
        make = next(iter(makes_upper))
        lo, hi = min(ccs), max(ccs)
        cc_str = f"{lo}-{hi}" if lo != hi else str(lo)
        tag_str = ", ".join(_sort_line_tags(line_tags))
        s = f"{make} {cc_str} ({tag_str}) {y_part}"
        return s.upper()

    # Eén merk + cc maar geen herkende lijn (alleen street of exotisch)
    if len(makes_upper) == 1 and ccs and y_part:
        make = next(iter(makes_upper))
        lo, hi = min(ccs), max(ccs)
        cc_str = f"{lo}-{hi}" if lo != hi else str(lo)
        s = f"{make} {cc_str} {y_part}"
        return s.upper()

    # Geen cc: korte fallback
    if y_part:
        s = f"{make_part} — {y_part}"
    else:
        s = make_part
    return s.upper()


def export_product_metafields_csv(
    path: str,
    structure_index: dict,
    relations: dict,
    product_rows: list[dict],
    handle_to_product_id: dict[str, str],
    shopify_merge: dict[str, dict[str, str]] | None = None,
    xml_file: str | None = None,
) -> tuple[int, int]:
    """
    Write one row per unique product handle from product_rows **plus** handles only in shopify_merge.
    YMM / fits_on columns are filled when any variant SKU has bike fitment in XML
    (MODELL + ZBH2BIKE), otherwise optionally from shopify_merge.

    Returns (total_rows, rows_with_non_empty_fits_on).
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    shopify_merge = shopify_merge or {}
    sku_to_ymm = build_merged_sku_to_ymm(
        structure_index, relations, xml_file=xml_file or XML_FILE
    )
    handle_to_skus = _build_handle_to_skus(product_rows)
    handle_to_title = _build_handle_to_title(product_rows)

    all_handles = set(handle_to_skus.keys()) | set(shopify_merge.keys())

    def _xml_fits_on(h: str) -> bool:
        for sku in handle_to_skus.get(h, []):
            if sku_to_ymm.get(sku):
                return True
        return False

    def _merge_fits_on(h: str) -> bool:
        return bool((shopify_merge.get(h, {}).get("fits_on") or "").strip())

    def _any_fits_on(h: str) -> bool:
        return _xml_fits_on(h) or _merge_fits_on(h)

    # Regels mét fits_on eerst (XML of merge)
    handles_sorted = sorted(all_handles, key=lambda h: (0 if _any_fits_on(h) else 1, h))

    rows_written = 0
    with_fits = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(METAFIELDS_HEADER)
        for handle in handles_sorted:
            merge_row = shopify_merge.get(handle, {})
            title = handle_to_title.get(handle, "") or merge_row.get("title", "")
            product_id = (
                handle_to_product_id.get(handle, "")
                or merge_row.get("id", "")
                or ""
            ).replace("~", "").strip()
            skus = handle_to_skus.get(handle, [])
            ymm_union: set[tuple[str, str, str]] = set()
            for sku in skus:
                ymm_union.update(sku_to_ymm.get(sku, set()))

            if ymm_union:
                fits_on = _ymm_tuples_to_fits_on_json(ymm_union)
                all_years = {t[2] for t in ymm_union}
                all_makes = {t[0] for t in ymm_union}
                all_models = {t[1] for t in ymm_union}
                fits_on_year = _pipe_join_sorted(all_years)
                fits_on_make = _pipe_join_sorted(all_makes)
                fits_on_model = _pipe_join_sorted(all_models)
                ymm_summary = _ymm_summary(ymm_union)
            else:
                fits_on = ""
                fits_on_year = ""
                fits_on_make = ""
                fits_on_model = ""
                ymm_summary = ""

            # Shopify-export als aanvulling (SKU ontbreekt in huidige XML of data is ouder/nieuw)
            if not fits_on.strip() and merge_row.get("fits_on", "").strip():
                fits_on = merge_row.get("fits_on", "").strip()
                fits_on_year = merge_row.get("fits_on_year", "") or fits_on_year
                fits_on_make = merge_row.get("fits_on_make", "") or fits_on_make
                fits_on_model = merge_row.get("fits_on_model", "") or fits_on_model
                ymm_summary = merge_row.get("ymm_summary", "") or ymm_summary

            if fits_on.strip():
                with_fits += 1

            mpn = sorted(skus)[0] if skus else merge_row.get("mpn", "")

            # Zelfde als typische Metafields Manager-export: inhoud vanaf fits_on in HOOFDLETTERS (niet de kolomkoppen).
            fo_y_new = merge_row.get("fits_on_year_new", "")
            fo_mk_new = merge_row.get("fits_on_make_new", "")
            fo_md_new = merge_row.get("fits_on_model_new", "")
            p_comp = merge_row.get("parts_compartment", "")
            p_floor = merge_row.get("parts_floor", "")
            p_shelve = merge_row.get("parts_shelve", "")
            p_list = merge_row.get("parts_list", "")
            p_box = merge_row.get("parts_box", "")
            p_row = merge_row.get("parts_row", "")
            g_mk = merge_row.get("global_fits_on_make_new", "")
            g_md = merge_row.get("global_fits_on_model_new", "")
            g_yr = merge_row.get("global_fits_on_year_new", "")

            w.writerow(
                [
                    product_id,
                    handle,
                    title,
                    _upper_fits_on_json_cell(fits_on),
                    _upper_plain_metafield_cell(fits_on_year),
                    _upper_plain_metafield_cell(fits_on_make),
                    _upper_plain_metafield_cell(fits_on_model),
                    _upper_plain_metafield_cell(fo_y_new),
                    _upper_plain_metafield_cell(fo_mk_new),
                    _upper_plain_metafield_cell(fo_md_new),
                    _upper_plain_metafield_cell(ymm_summary),
                    _upper_plain_metafield_cell(p_comp),
                    _upper_plain_metafield_cell(p_floor),
                    _upper_plain_metafield_cell(p_shelve),
                    _upper_plain_metafield_cell(p_list),
                    _upper_plain_metafield_cell(p_box),
                    _upper_plain_metafield_cell(p_row),
                    _upper_plain_metafield_cell(g_mk),
                    _upper_plain_metafield_cell(g_md),
                    _upper_plain_metafield_cell(g_yr),
                    _upper_plain_metafield_cell(mpn),
                ]
            )
            rows_written += 1
    return rows_written, with_fits


def run_metafields_export(
    product_ids_path: str | None = None,
    output_path: str | None = None,
    shopify_merge_csv: str | None = None,
) -> tuple[str, int]:
    product_ids_path = product_ids_path or os.path.join(
        REPORT_OUTPUT_DIR, "product_ids_from_xml.csv"
    )
    output_path = output_path or os.path.join(
        REPORT_OUTPUT_DIR, "product_metafields_metafields_manager.csv"
    )
    merge_map = load_shopify_product_merge_csv(shopify_merge_csv)
    if merge_map:
        print(
            f"Shopify-merge geladen: {len(merge_map)} handles uit {shopify_merge_csv}",
            flush=True,
        )
    print("XML inlezen voor Metafields-export (1e pass: structuur)…", flush=True)
    structure_index, relations = stream_xml_for_export()
    product_rows = build_product_rows(structure_index, relations)
    handle_to_product_id = build_handle_to_product_id(product_ids_path)
    print(
        "Tweede XML-pass (ZBH2BIKE) voor fits_on / YMM…",
        flush=True,
    )
    n, n_fits = export_product_metafields_csv(
        output_path,
        structure_index,
        relations,
        product_rows,
        handle_to_product_id,
        shopify_merge=merge_map,
    )
    print(
        f"Metafields: {n} productregels, waarvan {n_fits} met fits_on "
        f"(YMM: Bikes MODELL + ZBH2BIKE).",
        flush=True,
    )
    return output_path, n
