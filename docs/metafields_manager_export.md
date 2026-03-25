# Metafields Manager – product export uit KTM XML

Compatibel met het importformaat zoals [Metafields Manager](https://metafieldsmanager.thebestagency.com/docs) gebruikt voor product-metafields (`fits_on`, `fits_on_year`, `fits_on_make`, `fits_on_model`, `ymm_summary`, enz.).

## Stappenplan

### Aanbevolen: alleen delta (na import van nieuwe producten)

Kleine bestanden, minder kans op time-outs in YMM-/Metafields-apps.

1. **Delta-CSV** met kolom `Handle`: `main.py` schrijft die naar `output/products/shopify_export_delta_<timestamp>.csv`. Neem het bestand van de run waarmee je de betreffende producten hebt geëxporteerd (of de nieuwste delta als die set klopt).

2. **YMM + Product Id’s (delta)** — cache verversen na een grote import:

   ```bash
   python3 -u scripts/export_product_ids_and_ymm.py --refresh-shopify-cache \
     --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
   ```

   Output o.a.: `output/ids/product_ids_from_xml_delta.csv`, `output/ymm/ymm_APP_import_DELTA.csv` (eventueel gesplitst in `_part_00x`).

3. **Metafields (delta)** — standaard wordt `output/ids/product_ids_from_xml_delta.csv` gebruikt (zelfde als stap 2). Alleen nodig om `--product-ids` mee te geven als je een ander bestand wilt:

   ```bash
   python3 -u scripts/export_product_metafields.py \
     --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
   ```

   Output: `output/metafields/product_metafields_metafields_manager_delta.csv`.

   In plaats van `--delta-handles-csv` kun je `--delta-handles-file pad/naar/handles.txt` gebruiken (één handle per regel, `#` = commentaar).

**Alternatief (geen nieuwe generatie):** heb je al de **volledige** `product_ids` in `output/ids/` plus YMM + metafields in `output/ymm/` en `output/metafields/`, filter dan met:

```bash
python3 scripts/export_delta_app_imports.py
```

Dat leest de **laatste** `output/products/shopify_export_delta_*.csv` en schrijft o.a. `output/ymm/ymm_APP_import_delta_latest.csv` en `output/metafields/product_metafields_delta_latest.csv`.

### Hele catalogus (grote bestanden)

1. `python3 -u scripts/export_product_ids_and_ymm.py` (optioneel `--refresh-shopify-cache`)
2. `python3 -u scripts/export_product_metafields.py`

## Output

- Volledig: `output/metafields/product_metafields_metafields_manager.csv`
- Delta: `output/metafields/product_metafields_metafields_manager_delta.csv`

## Opties

```bash
python3 scripts/export_product_metafields.py --product-ids output/ids/product_ids_from_xml.csv -o output/metafields/mijn_metafields.csv
```

## Wat wordt gevuld

| Kolom | Bron |
|--------|------|
| `handle`, `title` | XML-structuur + `build_product_rows` |
| `id` | `Product Id` uit `product_ids_from_xml.csv` (zelfde mapping als YMM-export) |
| `fits_on` | JSON `{ "MAKE": { "Model": ["year",…], … } }` uit alle variant-SKU’s: **Bikes MODELL** (`PRODUKT_ZU_STRUKTUR_ELEMENT`) **plus** inverse **ZBH2BIKE**-lijsten op motor-PRODUKT’s (zie `docs/zbh2bike_ymm.md`). **Inhoud** (niet de kolomkoppen) vanaf `fits_on` t/m `MPN` wordt in **hoofdletters** geschreven, zoals in een typische Metafields Manager-export. |
| `fits_on_year`, `fits_on_make`, `fits_on_model` | Unieke waarden, gescheiden met `\|\|` (zoals je bestaande export) |
| `ymm_summary` | Als er cc + lijn-tags uit de modelnamen te halen valt: `KTM 125-500 (EXC, SX, XC, XCF) 2019-2023` — volgorde EXC→SX→XC→XCF; **XCF** alleen bij echte `«cc» XC-F` (niet `EXC-F`, dat hoort bij EXC). Anders korte fallback `KTM — 2019-2023`. |
| `MPN` | Eerste (alfabetisch) variant-SKU van het product |
| `parts_*`, `global_fits_on_*`, `fits_on_*_new` | Leeg (niet in KTM XML) |

Producten **zonder** bike-fitment in de XML krijgen een regel met lege `fits_on`-velden (zelfde idee als je voorbeeldregel met alleen `id`/`handle`/`title`).

**Let op:** veel regels hebben lege `fits_on` (kleding, accessoires, enz.). In de export staan regels **met** `fits_on` **bovenaan**, zodat je in Excel meteen data ziet. Na de run print het script hoeveel regels YMM hebben.

### Als `fits_on` in Shopify wél staat maar niet uit de XML komt

Sommige artikelen (bijv. `78005081000`) staan **niet** in jouw huidige `CBEXPDN_*.xml` → dan kan het script geen motor-fitment berekenen. Exporteer dan een **product-CSV uit Shopify** (met kolom **Handle** en je metafield **`fits_on`** of **Fits on**) en merge:

```bash
python3 -u scripts/export_product_metafields.py \
  --merge-from-shopify-csv ~/Downloads/shopify_products_export.csv
```

De merge vult **alleen** lege `fits_on` (XML heeft voorrang) en voegt **ontbrekende handles** toe die alleen in Shopify bestaan.

### ZBH2BIKE (kort)

Accessoires die in de boom vooral aan `$M-<sku>` hangen, krijgen `fits_on` mee via de onderdelenlijsten op complete motoren. Technische uitleg: `docs/zbh2bike_ymm.md`. Controleren: `python3 scripts/check_ymm_sku.py 00010000318`.

De **Shopify product-export** (`output/products/…`) wordt hiervoor niet aangepast — alleen `output/ids/`, `output/ymm/` en `output/metafields/` gebruiken deze logica.
