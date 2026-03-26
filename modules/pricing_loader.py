import csv
import io
import os

from config import INPUT_DIR, VAT_MULTIPLIER


def detect_0150_csv_delimiter(first_line: str) -> str:
    """Komma (huidige ERP-export) of puntkomma (oudere bestanden)."""
    for delim in (",", ";"):
        r = csv.reader(io.StringIO(first_line), delimiter=delim)
        row = next(r, [])
        if len(row) >= 10 and row[1].strip() == "ArticleNumber":
            return delim
    return ","


def load_price_index():

    price_index = {}
    barcode_index = {}
    status_index = {}

    price_file = None

    for f in os.listdir(INPUT_DIR):
        if "0150" in f and f.endswith(".csv"):
            price_file = f
            break

    if not price_file:
        raise FileNotFoundError("0150 prijsbestand niet gevonden.")

    path = os.path.join(INPUT_DIR, price_file)

    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

    for enc in encodings:
        try:
            with open(path, newline="", encoding=enc) as f:
                first = f.readline()
                f.seek(0)
                delim = detect_0150_csv_delimiter(first)
                reader = csv.reader(f, delimiter=delim)
                next(reader, None)

                for row in reader:
                    if len(row) < 24:
                        continue

                    sku = row[1].strip()  # Kolom B
                    price_raw = row[4].strip()  # Kolom E
                    article_status = row[10].strip()  # Kolom K
                    gtin = row[23].strip()  # Kolom X

                    if not sku:
                        continue

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
