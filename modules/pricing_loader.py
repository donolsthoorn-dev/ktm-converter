import csv
import glob
import io
import os
from pathlib import Path

import config
from config import VAT_MULTIPLIER

# Standaard KTM-export (input/); structuur van dit bestand is leidend voor kolomnamen.
DEFAULT_0150_CSV_NAME = "0150_35_Z1_EUR_EN_csv.csv"


def _find_0150_csv_path() -> str:
    """
    KTM: 1) KTM_0150_CSV, 2) DEFAULT onder input/, 3) eerste *0150*.csv.
    """
    input_dir = config.INPUT_DIR
    explicit = os.environ.get("KTM_0150_CSV", "").strip()
    if explicit:
        p = explicit if os.path.isabs(explicit) else os.path.join(input_dir, explicit)
        if os.path.isfile(p):
            return p
    default_path = os.path.join(input_dir, DEFAULT_0150_CSV_NAME)
    if os.path.isfile(default_path):
        return default_path
    if not os.path.isdir(input_dir):
        raise FileNotFoundError("0150 prijsbestand niet gevonden.")
    for f in os.listdir(input_dir):
        if "0150" in f and f.endswith(".csv"):
            return os.path.join(input_dir, f)
    raise FileNotFoundError("0150 prijsbestand niet gevonden.")


def _find_brand_price_csv_paths() -> list[str]:
    """Prijs-CSV's voor actief merk (HSQ: 1100 dan 0140; WP: 0910)."""
    brand = config.get_active_brand()
    if brand.id == "ktm":
        return [_find_0150_csv_path()]

    input_dir = config.INPUT_DIR
    explicit = os.environ.get(brand.price_csv_env_var, "").strip()
    if explicit:
        p = explicit if os.path.isabs(explicit) else os.path.join(input_dir, explicit)
        if os.path.isfile(p):
            return [p]

    paths: list[str] = []
    seen: set[str] = set()
    for name in brand.price_csv_names:
        p = os.path.join(input_dir, name)
        if os.path.isfile(p) and p not in seen:
            paths.append(p)
            seen.add(p)

    if os.path.isdir(input_dir):
        for pattern in brand.price_csv_fallback_globs:
            for p in sorted(glob.glob(os.path.join(input_dir, pattern))):
                if p not in seen:
                    paths.append(p)
                    seen.add(p)

    if not paths:
        codes = ", ".join(brand.price_csv_names)
        raise FileNotFoundError(
            f"Geen prijs-CSV voor merk '{brand.id}' in {input_dir} (verwacht o.a. {codes})."
        )
    return paths


def normalize_sku_key(sku: str | None) -> str:
    """Zelfde normalisatie als 0150-index (uppercase): XML en CSV kunnen qua casing verschillen."""
    return str(sku or "").strip().upper()


# WP CBEXPDN: verkoopeenheid in XML/Shopify (T05049-00) vs ERP ArticleNumber (T05049).
WP_XML_VARIANT_SUFFIX = "-00"


def pricelist_lookup_keys(sku: str | None) -> list[str]:
    """
    Sleutels om een XML/Shopify-SKU te matchen op de prijs-CSV (ArticleNumber).
    Probeert eerst exact, daarna zonder WP-verpakkingssuffix -00.
    """
    key = normalize_sku_key(sku)
    if not key:
        return []
    keys = [key]
    if key.endswith(WP_XML_VARIANT_SUFFIX) and len(key) > len(WP_XML_VARIANT_SUFFIX):
        base = key[: -len(WP_XML_VARIANT_SUFFIX)]
        if base and base not in keys:
            keys.append(base)
    return keys


def shopify_variant_lookup_keys(pricelist_sku: str | None) -> list[str]:
    """
    Sleutels in de variant-cache wanneer de ERP-SKU uit de prijs-CSV bekend is.
    Shopify/XML kan dezelfde artikelen onder Txxxx-00 registreren.
    """
    key = normalize_sku_key(pricelist_sku)
    if not key:
        return []
    keys = [key]
    xml_style = f"{key}{WP_XML_VARIANT_SUFFIX}"
    if xml_style not in keys:
        keys.append(xml_style)
    return keys


def lookup_in_str_index(index: dict[str, str], sku: str | None) -> str:
    """Eerste treffer in index (prijs, barcode, status) via pricelist_lookup_keys."""
    for k in pricelist_lookup_keys(sku):
        val = index.get(k)
        if val not in (None, ""):
            return str(val)
    return ""


def lookup_stock_code_in_index(index: dict[str, int], sku: str | None) -> int | None:
    """Eerste treffer in StockAvailable-index (0/1/2) via pricelist_lookup_keys."""
    for k in pricelist_lookup_keys(sku):
        if k in index:
            return index[k]
    return None


def erp_sku_keys_for_active_brand() -> set[str] | None:
    """
    WP: ArticleNumber-set uit prijs-CSV voor XML→ERP SKU-normalisatie (T05049-00 → T05049).
    Andere merken: None (geen -00-stripping).
    """
    if config.get_active_brand().id != "wp":
        return None
    try:
        price_index, _, _ = load_price_index()
    except FileNotFoundError:
        return None
    return set(price_index.keys()) if price_index else None


def canonical_erp_sku_from_xml(
    sku: str | None,
    *,
    erp_sku_keys: set[str] | None = None,
) -> str:
    """
    CBEXPDN-verpakkingssuffix -00 verwijderen als de basis-SKU in de ERP-prijslijst staat.
    Zonder erp_sku_keys (niet-WP of geen CSV): alleen uppercase, geen wijziging.
    """
    key = normalize_sku_key(sku)
    if not key or erp_sku_keys is None:
        return key
    if not key.endswith(WP_XML_VARIANT_SUFFIX) or len(key) <= len(WP_XML_VARIANT_SUFFIX):
        return key
    base = key[: -len(WP_XML_VARIANT_SUFFIX)]
    if base and base in erp_sku_keys:
        return base
    return key


def variant_pairs_for_pricelist_sku(
    sku_to_vp: dict[str, list[tuple[str, str | None]]],
    pricelist_sku: str,
) -> list[tuple[str, str | None]]:
    """Alle (variant_id, product_id)-paren voor een ERP-SKU, incl. XML-alias (-00)."""
    seen: set[str] = set()
    out: list[tuple[str, str | None]] = []
    for k in shopify_variant_lookup_keys(pricelist_sku):
        for vid, pid in sku_to_vp.get(k) or []:
            if vid and vid not in seen:
                seen.add(vid)
                out.append((vid, pid))
    return out


def detect_0150_csv_delimiter(first_line: str) -> str:
    """Komma (huidige ERP-export) of puntkomma (oudere bestanden)."""
    for delim in (",", ";"):
        r = csv.reader(io.StringIO(first_line), delimiter=delim)
        row = next(r, [])
        # Korte exports hebben soms <10 kolommen; ArticleNumber volstaat als signaal.
        if len(row) >= 2 and any(c.strip().lower() == "articlenumber" for c in row):
            return delim
    return ","


def _header_index_ci(header: list[str], names: tuple[str, ...], default: int) -> int:
    """Eerste kolom waarvan de naam (case-insensitive) overeenkomt met één van names."""
    lower_to_i: dict[str, int] = {}
    for i, cell in enumerate(header):
        key = cell.strip().lower()
        if key and key not in lower_to_i:
            lower_to_i[key] = i
    for n in names:
        k = n.strip().lower()
        if k in lower_to_i:
            return lower_to_i[k]
    return default


def _resolve_0150_column_indices(
    header: list[str],
) -> tuple[int, int, int, int, int | None]:
    """
    Kolommen op naam (zelfde idee als shopify_sync_from_pricelist_csv.read_pricelist_csv_desired).
    Fallback: vaste indices uit oudere vaste-layout export (B,E,K,V,X).
    """
    h = [x.strip() for x in header]

    sku_col = _header_index_ci(h, ("ArticleNumber",), 1)
    price_col = _header_index_ci(h, ("SalesPrice",), 4)
    status_col = _header_index_ci(h, ("ArticleStatus",), 10)
    stock_col = _header_index_ci(h, ("StockAvailable",), 21)

    gtin_col: int | None = None
    for nm in ("GTIN", "GTIN13", "EAN", "GlobalTradeItemNumber", "Barcode"):
        j = _header_index_ci(h, (nm,), -1)
        if j >= 0:
            gtin_col = j
            break
    if gtin_col is None or gtin_col < 0:
        gtin_col = 23 if len(h) > 23 else None

    return sku_col, price_col, status_col, stock_col, gtin_col


def _read_article_status_from_single_0150_style_csv(path: str) -> dict[str, str]:
    """Lees één KTM-export-CSV; return ArticleNumber (upper) -> ArticleStatus (string)."""
    out: dict[str, str] = {}
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc) as f:
                first = f.readline()
                f.seek(0)
                delim = detect_0150_csv_delimiter(first)
                reader = csv.reader(f, delimiter=delim)
                header = next(reader, None)
                if not header:
                    continue
                header_len = len(header)
                sku_col, _price_col, status_col, _stock_col, _gtin_col = (
                    _resolve_0150_column_indices(header)
                )
                min_len = max(sku_col, status_col) + 1
                for row in reader:
                    if len(row) < header_len:
                        row = list(row) + [""] * (header_len - len(row))
                    if len(row) < min_len:
                        continue
                    sku_raw = row[sku_col].strip()
                    if not sku_raw:
                        continue
                    out[sku_raw.upper()] = row[status_col].strip()
            return out
        except UnicodeDecodeError:
            continue
        except OSError:
            return out
    return out


def _read_stock_available_from_single_0150_style_csv(path: str) -> dict[str, int]:
    """Lees één KTM-export-CSV; return ArticleNumber (upper) -> StockAvailable (0/1/2)."""
    out: dict[str, int] = {}
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc) as f:
                first = f.readline()
                f.seek(0)
                delim = detect_0150_csv_delimiter(first)
                reader = csv.reader(f, delimiter=delim)
                header = next(reader, None)
                if not header:
                    continue
                header_len = len(header)
                sku_col, _price_col, _status_col, stock_col, _gtin_col = (
                    _resolve_0150_column_indices(header)
                )
                min_len = max(sku_col, stock_col) + 1
                for row in reader:
                    if len(row) < header_len:
                        row = list(row) + [""] * (header_len - len(row))
                    if len(row) < min_len:
                        continue
                    sku_raw = row[sku_col].strip()
                    if not sku_raw:
                        continue
                    raw = (row[stock_col] or "").strip()
                    if not raw:
                        continue
                    try:
                        code = int(float(raw.replace(",", ".")))
                    except ValueError:
                        continue
                    if code in (0, 1, 2):
                        out[sku_raw.upper()] = code
            return out
        except UnicodeDecodeError:
            continue
        except OSError:
            return out
    return out


def _resolve_35_z1_csv_paths_multi_brand(
    input_dir: str | None = None,
    *,
    project_root: str | None = None,
) -> list[str]:
    """
    Paden naar alle merk-prijs-CSV's (KTM 0150, HSQ 1100/0140, WP 0910).

    Standaard: PRICELIST_CSV_MERGE_ORDER t.o.v. projectroot (cwd of project_root).
    input_dir blijft als legacy-fallback: als er geen multi-brand bestanden zijn,
    glob *35_Z1_EUR_EN_csv.csv in die map (tests / lokale single-dir setups).
    """
    from modules.brand_config import resolve_pricelist_csv_paths

    root = Path(project_root) if project_root else Path.cwd()
    paths = resolve_pricelist_csv_paths(root)
    if paths:
        return [str(p) for p in paths]

    base = os.path.normpath(input_dir or config.INPUT_DIR)
    pattern = os.path.join(base, "*35_Z1_EUR_EN_csv.csv")
    return sorted(glob.glob(pattern))


def load_stock_available_from_35_z1_csv_files(
    input_dir: str | None = None,
    *,
    project_root: str | None = None,
) -> dict[str, int]:
    """
    ArticleNumber → StockAvailable (0/1/2) uit alle merk-prijs-CSV's (KTM+HSQ+WP).

    Merge-volgorde: PRICELIST_CSV_MERGE_ORDER (later bestand wint bij dubbele SKU).
    input_dir: alleen legacy-fallback als multi-brand paden ontbreken.
    """
    paths = _resolve_35_z1_csv_paths_multi_brand(
        input_dir, project_root=project_root
    )
    merged: dict[str, int] = {}
    for path in paths:
        merged.update(_read_stock_available_from_single_0150_style_csv(path))
    return merged


def load_article_status_from_35_z1_csv_files(
    input_dir: str | None = None,
    *,
    project_root: str | None = None,
) -> dict[str, str]:
    """
    ArticleNumber → ArticleStatus uit alle merk-prijs-CSV's (KTM+HSQ+WP).

    Merge-volgorde: PRICELIST_CSV_MERGE_ORDER (later bestand wint bij dubbele SKU).
    input_dir: alleen legacy-fallback als multi-brand paden ontbreken.
    """
    paths = _resolve_35_z1_csv_paths_multi_brand(
        input_dir, project_root=project_root
    )
    merged: dict[str, str] = {}
    for path in paths:
        merged.update(_read_article_status_from_single_0150_style_csv(path))
    return merged


def _merge_price_row(
    row: list[str],
    header_len: int,
    sku_col: int,
    price_col: int,
    status_col: int,
    gtin_col: int | None,
    price_index: dict[str, str],
    barcode_index: dict[str, str],
    status_index: dict[str, str],
) -> None:
    if len(row) < header_len:
        row = list(row) + [""] * (header_len - len(row))
    min_len = max(sku_col, price_col, status_col) + 1
    if len(row) < min_len:
        return

    sku_raw = row[sku_col].strip()
    if not sku_raw:
        return
    sku = sku_raw.upper()
    price_raw = row[price_col].strip()
    article_status = row[status_col].strip()
    gtin = ""
    if gtin_col is not None and gtin_col < len(row):
        gtin = row[gtin_col].strip()

    if price_raw:
        try:
            base_price = float(price_raw.replace(",", "."))
            final_price = round(base_price * VAT_MULTIPLIER, 2)
            price_index[sku] = f"{final_price:.2f}"
        except ValueError:
            pass

    if gtin and gtin.isdigit():
        barcode_index[sku] = gtin

    status_index[sku] = article_status


def _load_single_price_csv(path: str) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    price_index: dict[str, str] = {}
    barcode_index: dict[str, str] = {}
    status_index: dict[str, str] = {}
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc) as f:
                first = f.readline()
                f.seek(0)
                delim = detect_0150_csv_delimiter(first)
                reader = csv.reader(f, delimiter=delim)
                header = next(reader, None)
                if not header:
                    continue

                header_len = len(header)
                sku_col, price_col, status_col, _stock_col, gtin_col = (
                    _resolve_0150_column_indices(header)
                )

                for row in reader:
                    _merge_price_row(
                        row,
                        header_len,
                        sku_col,
                        price_col,
                        status_col,
                        gtin_col,
                        price_index,
                        barcode_index,
                        status_index,
                    )
            return price_index, barcode_index, status_index
        except UnicodeDecodeError:
            continue

    raise RuntimeError(f"Prijsbestand kon niet worden gelezen: {path}")


def load_price_index():
    price_index: dict[str, str] = {}
    barcode_index: dict[str, str] = {}
    status_index: dict[str, str] = {}

    paths = _find_brand_price_csv_paths()
    for path in paths:
        part_price, part_barcode, part_status = _load_single_price_csv(path)
        price_index.update(part_price)
        barcode_index.update(part_barcode)
        status_index.update(part_status)
        if len(paths) > 1:
            print(f"  prijs-CSV: {os.path.basename(path)} ({len(part_price)} regels)")

    print(f"{len(price_index)} prijzen ingelezen.")
    print(f"{len(barcode_index)} barcodes ingelezen.")
    print(f"{len(status_index)} artikelstatussen ingelezen.")

    return price_index, barcode_index, status_index
