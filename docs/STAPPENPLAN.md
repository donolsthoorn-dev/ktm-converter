# Stappenplan — KTM, Husqvarna (HSQ) en WP

**Dit is het hoofddocument.** Alles in volgorde, alle merken in één run.

Werk altijd vanaf de projectmap:

```bash
cd ~/Documents/ktm_project
```

Eenmalig: `.env` met `SHOPIFY_ACCESS_TOKEN` (zie [`shopify_env.md`](shopify_env.md)).

---

## 1. Haal databestanden op en zet ze in de juiste map

**Automatisch (FTP):**

```bash
python3 scripts/fetch_input_sftp.py
python3 scripts/prepare_input_from_ftp.py --extract-xml-from-zips
```

**Of handmatig** — zet deze bestanden in de map:

| Merk | Map | XML | Prijs-CSV |
|------|-----|-----|-----------|
| **KTM** | `input/` | `CBEXPDN_KTM-DN*.xml` | `0150_35_Z1_EUR_EN_csv.csv` (of `*0150*.csv`) |
| **HSQ** | `input/hsq/` | `CBEXPDN*.xml` (bv. `CBEXPDN_HQV-DN-….xml`) | `1100_35_Z1_EUR_EN_csv.csv` **en** `0140_35_Z1_EUR_EN_csv.csv` |
| **WP** | `input/wp/` | `CBEXPDN*.xml` (bv. `CBEXPDN_WP-DN-….xml`) | `0910_35_Z1_EUR_EN_csv.csv` (of `*0910*.csv`) |

Optioneel: productafbeeldingen in `input/` (HSQ/WP gebruiken ook afbeeldingen uit `input/`).

---

## 2. Genereer Shopify product-CSV’s (`main.py`)

Drie keer achter elkaar in dezelfde terminal (of drie tabbladen tegelijk):

```bash
python3 -u main.py
python3 -u main.py --brand hsq
python3 -u main.py --brand wp
```

**Output (delta = importeren in Shopify):**

| Merk | Delta-CSV |
|------|-----------|
| KTM | `output/products/shopify_export_delta_*.csv` |
| HSQ | `output/hsq/products/shopify_export_delta_*.csv` |
| WP | `output/wp/products/shopify_export_delta_*.csv` |

Laatste delta vinden:

```bash
ls -t output/products/shopify_export_delta_*.csv | head -1
ls -t output/hsq/products/shopify_export_delta_*.csv | head -1
ls -t output/wp/products/shopify_export_delta_*.csv | head -1
```

---

## 3. Upload producten handmatig naar Shopify

**Shopify Admin → Products → Import**

Importeer per merk het **delta**-bestand uit stap 2:

| Merk | Bestand |
|------|---------|
| KTM | `output/products/shopify_export_delta_….csv` |
| HSQ | `output/hsq/products/shopify_export_delta_….csv` |
| WP | `output/wp/products/shopify_export_delta_….csv` |

**Wacht** tot elke import **klaar** is voordat je stap 4 doet.

---

## 4. Genereer YMM- en Metafields-CSV’s (Terminal)

Plak dit blok **in één keer** (eerst alle YMM, dan alle metafields).  
`--refresh-shopify-cache` alleen bij KTM (eerste run); HSQ en WP gebruiken dezelfde cache.

```bash
python3 -u scripts/export_product_ids_and_ymm.py --brand ktm --refresh-shopify-cache
python3 -u scripts/export_product_ids_and_ymm.py --brand hsq
python3 -u scripts/export_product_ids_and_ymm.py --brand wp

python3 -u scripts/export_product_metafields.py --brand ktm
python3 -u scripts/export_product_metafields.py --brand hsq
python3 -u scripts/export_product_metafields.py --brand wp
```

**Cross-brand fitment (standaard):** YMM en Metafields gebruiken per SKU de **union** van KTM + HSQ + WP XML (zelfde onderdeel-SKU → dezelfde `fits_on` op alle drie Shopify-handles). Uitzetten: `--no-cross-brand-ymm` op beide export-scripts.

**Alleen nieuwe producten** (na delta-import): voeg per merk `--delta-handles-csv` toe met het delta-bestand van stap 2. Voorbeeld KTM:

```bash
python3 -u scripts/export_product_ids_and_ymm.py --brand ktm --refresh-shopify-cache \
  --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
```

**Output:**

| Merk | YMM-app | Metafields Manager |
|------|---------|-------------------|
| KTM | `output/ymm/ymm_APP_import_*.csv` | `output/metafields/product_metafields_metafields_manager*.csv` |
| HSQ | `output/hsq/ymm/` | `output/hsq/metafields/` |
| WP | `output/wp/ymm/` | `output/wp/metafields/` |

---

## 5. Importeer YMM handmatig in de YMM-app

Per merk het bestand uit `output/…/ymm/` (bij delta-import vaak `ymm_APP_import_DELTA*.csv` of `_part_001`).

Eén import tegelijk in de app aanbevolen.

---

## 6. Importeer Metafields handmatig (Metafields Manager)

Per merk het CSV-bestand uit `output/…/metafields/` (bijv. `product_metafields_metafields_manager_delta.csv` of `_part_001.csv`).

Eén import tegelijk aanbevolen.

---

## Optioneel — alleen KTM: prijzen / ETA via API

Na nieuwe producten in Shopify:

```bash
python3 scripts/shopify_refresh_variant_cache.py
python3 scripts/shopify_sync_from_pricelist_csv.py
```

---

## Automatisch (hoef je niet bij elke productronde)

| Wat | Waar |
|-----|------|
| Shopify → Supabase (’s nachts) | GitHub Actions **Job worker** |
| Supabase YMM-projectie / backfill | [`supabase-ymm-pipeline.md`](supabase-ymm-pipeline.md) |

Dat is **niet** hetzelfde als stap 4–6 (XML → CSV → apps).

---

## Meer uitleg

| Onderwerp | Document |
|-----------|----------|
| Commando’s copy-paste | [`HOWTO.md`](../HOWTO.md) |
| FTP, fouten, ETA | [`workflow.md`](workflow.md) |
| Metafields/YMM detail | [`metafields_manager_export.md`](metafields_manager_export.md) |
| Per-merk bestandsnamen (uitgebreid) | [`workflow_per_merk.md`](workflow_per_merk.md) |
