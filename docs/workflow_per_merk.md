# Workflow per merk (KTM · Husqvarna · WP)

Stappenplan om **regelmatig** nieuwe producten te verwerken: bronbestanden → `main.py` → Shopify-import → YMM + metafields.

Alle commando’s vanaf de **projectroot** (`ktm_project/`). Python: `python3`.

**Snelle commando’s (alle merken):** [`HOWTO.md`](../HOWTO.md)  
**FTP, fouten, ETA-sync:** [`docs/workflow.md`](workflow.md)  
**Alleen nieuwe producten (zonder ETA-API):** [`docs/workflow_nieuwe_producten.txt`](workflow_nieuwe_producten.txt)

---

## Voor je begint

1. Terminal openen in de projectmap (`cd …/ktm_project`).
2. Eenmalig: `.env` met o.a. `SHOPIFY_ACCESS_TOKEN` (zie [`docs/shopify_env.md`](shopify_env.md)).
3. Na elke Shopify-productimport: **wacht tot de import klaar is** voordat je YMM/metafields draait.

**Merk kiezen in `main.py` en scripts:** `--brand ktm` (default), `--brand hsq`, of `--brand wp`  
(Niet `brands` — het is één vlag `--brand` / `-b`.)

---

## Bronbestanden ophalen (alle merken)

**Automatisch (FTP/FTPS):**

```bash
python3 scripts/fetch_input_sftp.py
python3 scripts/prepare_input_from_ftp.py --extract-xml-from-zips
```

`prepare_input_from_ftp.py` zet prijs-CSV’s automatisch in de juiste map (zie tabel hieronder). XML in een zip wordt uitgepakt naar de map die je met `--input-dir` kiest (standaard `input/`); voor HSQ/WP kun je XML handmatig in `input/hsq` of `input/wp` zetten, of na extract verplaatsen.

**Handmatig:** zet de bestanden uit de tabel in de genoemde map. Controleer daarna of er precies **één** XML matcht (of zet het pad in `.env`, zie kolom *Override*).

| Merk | Map | XML (glob) | Prijs-CSV (voorkeursnaam) | Fallback glob |
|------|-----|------------|---------------------------|---------------|
| **KTM** | `input/` | `CBEXPDN_KTM-DN*.xml` | `0150_35_Z1_EUR_EN_csv.csv` | `*0150*.csv` |
| **HSQ** | `input/hsq/` | `CBEXPDN*.xml` (bv. `CBEXPDN_HQV-DN-….xml`) | `1100_35_Z1_EUR_EN_csv.csv` + `0140_35_Z1_EUR_EN_csv.csv` (0140 wint bij dubbele SKU) | `*1100*.csv`, `*0140*.csv` |
| **WP** | `input/wp/` | `CBEXPDN*.xml` (bv. `CBEXPDN_WP-DN-….xml`) | `0910_35_Z1_EUR_EN_csv.csv` | `*0910*.csv` |

Optioneel voor alle merken: productafbeeldingen onder `input/` (recursief op bestandsnaam). HSQ/WP gebruiken ook afbeeldingen uit de gedeelde `input/` (KTM-map).

**.env override (optioneel):** `KTM_XML_FILE`, `HSQ_XML_FILE`, `WP_XML_FILE` · `KTM_0150_CSV`, `HSQ_PRICE_CSV`, `WP_PRICE_CSV`

---

## KTM — checklist

### 1. Databestanden in `input/`

- [ ] XML: `CBEXPDN_KTM-DN*.xml`
- [ ] Prijs: `0150_35_Z1_EUR_EN_csv.csv` (of ander `*0150*.csv`)
- [ ] Optioneel: afbeeldingen in `input/`

### 2. Product-CSV genereren

```bash
python3 -u main.py
```

Of expliciet: `python3 -u main.py --brand ktm`

**Output (noteer het bestand met timestamp):**

- Delta (meestal importeren): `output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv`
- Volledig: `output/products/shopify_export_all_JJJJMMDD_HHMMSS.csv`

Laatste delta vinden:

```bash
ls -t output/products/shopify_export_delta_*.csv | head -1
```

### 3. Shopify — producten importeren

**Shopify Admin → Products → Import** met de **delta-CSV** uit stap 2. Wacht tot de import klaar is.

### 4. YMM + metafields (Terminal)

Vervang `JJJJMMDD_HHMMSS` door de timestamp van **jouw** delta-run.

```bash
python3 -u scripts/export_product_ids_and_ymm.py --refresh-shopify-cache \
  --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv

python3 -u scripts/export_product_metafields.py \
  --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
```

### 5. Apps uploaden

| App | Bestand(en) |
|-----|-------------|
| **YMM-app** | `output/ymm/ymm_APP_import_DELTA*.csv` (eventueel `_part_00x`) |
| **Metafields Manager** | `output/metafields/product_metafields_metafields_manager_delta.csv` |

**Alternatief** (alleen filteren, geen nieuwe generatie):  
`python3 scripts/export_delta_app_imports.py` → o.a. `output/ymm/ymm_APP_import_delta_latest.csv`

### 6. Optioneel — prijzen / ETA / voorraadbeleid via API (KTM)

Na **nieuwe** producten in Shopify:

```bash
python3 scripts/shopify_refresh_variant_cache.py
python3 scripts/shopify_sync_from_pricelist_csv.py
```

Zie [`HOWTO.md`](../HOWTO.md) en [`docs/workflow.md`](workflow.md) §3b.

---

## Husqvarna (HSQ) — checklist

### 1. Databestanden in `input/hsq/`

- [ ] XML: `CBEXPDN*.xml` (Husqvarna-export)
- [ ] Prijs: `1100_35_Z1_EUR_EN_csv.csv` en `0140_35_Z1_EUR_EN_csv.csv`
- [ ] Optioneel: afbeeldingen (`input/hsq/` en/of gedeelde `input/`)

### 2. Product-CSV genereren

```bash
python3 -u main.py --brand hsq
```

**Output:**

- Delta: `output/hsq/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv`

```bash
ls -t output/hsq/products/shopify_export_delta_*.csv | head -1
```

### 3. Shopify — producten importeren

Importeer de delta-CSV uit `output/hsq/products/`. Handles hebben prefix `hsq-`.

### 4. YMM + metafields

```bash
python3 -u scripts/export_product_ids_and_ymm.py --brand hsq --refresh-shopify-cache \
  --delta-handles-csv output/hsq/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv

python3 -u scripts/export_product_metafields.py --brand hsq \
  --delta-handles-csv output/hsq/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
```

### 5. Apps uploaden

| App | Map |
|-----|-----|
| **YMM-app** | `output/hsq/ymm/` |
| **Metafields Manager** | `output/hsq/metafields/product_metafields_metafields_manager_delta.csv` |

Compact filter: `python3 scripts/export_delta_app_imports.py --brand hsq`

---

## WP — checklist

### 1. Databestanden in `input/wp/`

- [ ] XML: `CBEXPDN*.xml` (WP-export)
- [ ] Prijs: `0910_35_Z1_EUR_EN_csv.csv` (of `*0910*.csv`)
- [ ] Optioneel: afbeeldingen (`input/wp/` en/of gedeelde `input/`)

### 2. Product-CSV genereren

```bash
python3 -u main.py --brand wp
```

**Output:**

- Delta: `output/wp/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv`

```bash
ls -t output/wp/products/shopify_export_delta_*.csv | head -1
```

### 3. Shopify — producten importeren

Importeer de delta-CSV uit `output/wp/products/`. Handles hebben prefix `wp-`.

### 4. YMM + metafields

```bash
python3 -u scripts/export_product_ids_and_ymm.py --brand wp --refresh-shopify-cache \
  --delta-handles-csv output/wp/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv

python3 -u scripts/export_product_metafields.py --brand wp \
  --delta-handles-csv output/wp/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
```

### 5. Apps uploaden

| App | Map |
|-----|-----|
| **YMM-app** | `output/wp/ymm/` |
| **Metafields Manager** | `output/wp/metafields/product_metafields_metafields_manager_delta.csv` |

Compact filter: `python3 scripts/export_delta_app_imports.py --brand wp`

---

## Volgorde in één oogopslag

```text
Bronbestanden (input/<merk>/)  →  main.py [--brand …]  →  Shopify delta-import
       →  export_product_ids_and_ymm.py  →  export_product_metafields.py
       →  YMM-app + Metafields Manager
```

| Stap | KTM | HSQ | WP |
|------|-----|-----|-----|
| Inputmap | `input/` | `input/hsq/` | `input/wp/` |
| `main.py` | `python3 -u main.py` | `… --brand hsq` | `… --brand wp` |
| Delta-CSV | `output/products/shopify_export_delta_*.csv` | `output/hsq/products/…` | `output/wp/products/…` |
| YMM/metafields | zonder `--brand` (default ktm) | `--brand hsq` | `--brand wp` |
| YMM-output | `output/ymm/` | `output/hsq/ymm/` | `output/wp/ymm/` |
| Metafields-output | `output/metafields/` | `output/hsq/metafields/` | `output/wp/metafields/` |

---

## Hele catalogus (zeldzaam)

Alleen als je bewust alles opnieuw wilt syncen — grote bestanden, meer kans op time-outs in apps:

```bash
python3 -u scripts/export_product_ids_and_ymm.py --brand hsq
python3 -u scripts/export_product_metafields.py --brand hsq
```

Details: [`docs/metafields_manager_export.md`](metafields_manager_export.md).

---

## Veelvoorkomende fouten

| Probleem | Oplossing |
|----------|-----------|
| `0150` / prijsbestand niet gevonden | Juiste CSV in de **merk-map** (zie tabel boven) |
| Verkeerde of oude XML | Eén duidelijke XML in de map; anders `KTM_XML_FILE` / `HSQ_XML_FILE` / `WP_XML_FILE` in `.env` |
| YMM vindt geen product-id’s na import | `--refresh-shopify-cache` op `export_product_ids_and_ymm.py` |
| Time-out in YMM-/metafields-app | Delta-flow met `--delta-handles-csv`, niet de volledige catalogus |
| `main.py brands hsq` werkt niet | Gebruik `--brand hsq` (met dubbele streepje) |

Log bij falende ETL: `output/logs/<merk>_etl_<timestamp>.log` — zie [`docs/workflow.md`](workflow.md).
