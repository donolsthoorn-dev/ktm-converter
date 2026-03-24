# Metafields Manager – product export uit KTM XML

Compatibel met het importformaat zoals [Metafields Manager](https://metafieldsmanager.thebestagency.com/docs) gebruikt voor product-metafields (`fits_on`, `fits_on_year`, `fits_on_make`, `fits_on_model`, `ymm_summary`, enz.).

## Stappen

1. **Eerst** Product Id’s vullen (zelfde bron als YMM):

   ```bash
   python3 -u scripts/export_product_ids_and_ymm.py
   ```

2. **Daarna** Metafields-CSV genereren:

   ```bash
   python3 -u scripts/export_product_metafields.py
   ```

## Output

- Standaard: `output/reports/product_metafields_metafields_manager.csv`

## Opties

```bash
python3 scripts/export_product_metafields.py --product-ids output/reports/product_ids_from_xml.csv -o output/reports/mijn_metafields.csv
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

De **Shopify product-export** (`output/shopify/…`) wordt hiervoor niet aangepast — alleen `output/reports/` YMM/Metafields-exports gebruiken deze logica.
