import csv
import io
import os

from config import INPUT_DIR, VAT_MULTIPLIER

# Standaard KTM-export (input/); structuur van dit bestand is leidend voor kolomnamen.
DEFAULT_0150_CSV_NAME = "0150_35_Z1_EUR_EN_csv.csv"


def _find_0150_csv_path() -> str:
    """
    1) KTM_0150_CSV (absoluut pad of bestandsnaam onder input/)
    2) Anders input/DEFAULT_0150_CSV_NAME als dat bestand bestaat
    3) Anders eerste *0150*.csv in input/ (fallback)
    """
    explicit = os.environ.get("KTM_0150_CSV", "").strip()
    if explicit:
        p = explicit if os.path.isabs(explicit) else os.path.join(INPUT_DIR, explicit)
        if os.path.isfile(p):
            return p
    default_path = os.path.join(INPUT_DIR, DEFAULT_0150_CSV_NAME)
    if os.path.isfile(default_path):
        return default_path
    for f in os.listdir(INPUT_DIR):
        if "0150" in f and f.endswith(".csv"):
            return os.path.join(INPUT_DIR, f)
    raise FileNotFoundError("0150 prijsbestand niet gevonden.")


def normalize_sku_key(sku: str | None) -> str:
    """Zelfde normalisatie als 0150-index (uppercase): XML en CSV kunnen qua casing verschillen."""
    return str(sku or "").strip().upper()


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


def _resolve_0150_column_indices(header: list[str]) -> tuple[int, int, int, int | None]:
    """
    Kolommen op naam (zelfde idee als shopify_sync_from_0150.read_0150_desired).
    Fallback: vaste indices uit oudere vaste-layout export (B,E,K,X).
    """
    h = [x.strip() for x in header]

    sku_col = _header_index_ci(h, ("ArticleNumber",), 1)
    price_col = _header_index_ci(h, ("SalesPrice",), 4)
    status_col = _header_index_ci(h, ("ArticleStatus",), 10)

    gtin_col: int | None = None
    for nm in ("GTIN", "GTIN13", "EAN", "GlobalTradeItemNumber", "Barcode"):
        j = _header_index_ci(h, (nm,), -1)
        if j >= 0:
            gtin_col = j
            break
    if gtin_col is None or gtin_col < 0:
        gtin_col = 23 if len(h) > 23 else None

    return sku_col, price_col, status_col, gtin_col


def load_price_index():

    price_index = {}
    barcode_index = {}
    status_index = {}

    path = _find_0150_csv_path()

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
                sku_col, price_col, status_col, gtin_col = _resolve_0150_column_indices(header)
                min_len = max(sku_col, price_col, status_col) + 1

                for row in reader:
                    # Trailing lege velden ontbreken soms in de parse; vul aan t.o.v. header.
                    if len(row) < header_len:
                        row = list(row) + [""] * (header_len - len(row))
                    if len(row) < min_len:
                        continue

                    sku_raw = row[sku_col].strip()
                    if not sku_raw:
                        continue
                    sku = sku_raw.upper()
                    price_raw = row[price_col].strip()
                    article_status = row[status_col].strip()
                    gtin = ""
                    if gtin_col is not None:
                        gtin = row[gtin_col].strip()

                    # ---- PRICE ----
                    if price_raw:
                        try:
                            base_price = float(price_raw.replace(",", "."))
                            final_price = round(base_price * VAT_MULTIPLIER, 2)
                            price_index[sku] = f"{final_price:.2f}"
                        except ValueError:
                            pass

                    # ---- BARCODE ----
                    if gtin and gtin.isdigit():
                        barcode_index[sku] = gtin

                    # ---- ARTICLE STATUS ----
                    status_index[sku] = article_status

            print(f"{len(price_index)} prijzen ingelezen.")
            print(f"{len(barcode_index)} barcodes ingelezen.")
            print(f"{len(status_index)} artikelstatussen ingelezen.")

            return price_index, barcode_index, status_index

        except UnicodeDecodeError:
            continue

    raise RuntimeError("Prijsbestand kon niet worden gelezen.")
