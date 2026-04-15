# Operationeel: van FTP naar Shopify en apps

Architectuur en technische context: **`PROJECT_CONTEXT_KTM.md`**.  
Metafields/YMM-details: **`docs/metafields_manager_export.md`**, **`docs/zbh2bike_ymm.md`**.  
Shopify-env: **`docs/shopify_env.md`**. Caches: **`docs/shopify_cache_en_scheduling.md`**.

### Bij falende ETL / import (`main.py`)

1. Lees de **console** (tracebacks, foutmeldingen).
2. Open het **logbestand** van die run: standaard `output/logs/ktm_etl_<timestamp>.log` (de timestamp hoort bij het starttijdstip; zie ook de regel *Logbestand:* aan het begin van de run). Optioneel: eigen pad via `KTM_LOG_FILE` in `.env`.
3. Controleer **`input/`**: XML (`CBEXPDN_…xml`), minstens één `*0150*.csv`, en of afbeeldingen verwacht onder `input/` staan.
4. Controleer **`.env`**: o.a. `SHOPIFY_ACCESS_TOKEN` en CDN/shop (`docs/shopify_env.md`).
5. Logniveau verhogen voor diagnose: `KTM_LOG_LEVEL=DEBUG` (alleen tijdelijk).

Python-versie: zie `requires-python` in `pyproject.toml` (≥ 3.10).

---

## Ultra korte versie

1. FTP/FTPS: `python3 scripts/fetch_input_sftp.py`
2. Staging → input: `python3 scripts/prepare_input_from_ftp.py --extract-xml-from-zips`
3. Export: `python3 -u main.py`
4. Shopify Admin → Products → Import: `output/products/shopify_export_delta_*.csv` (wacht tot klaar)
5. Delta YMM + metafields (aanbevolen; vervang de timestamp):

   ```bash
   python3 -u scripts/export_product_ids_and_ymm.py --refresh-shopify-cache \
     --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv

   python3 -u scripts/export_product_metafields.py \
     --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
   ```

6. Apps: YMM → `output/ymm/ymm_APP_import_DELTA*.csv`; Metafields Manager → `output/metafields/product_metafields_metafields_manager_delta.csv`
7. Optioneel KTM prijs-CSV → Shopify API (ETA): na nieuwe productimport eerst `python3 scripts/shopify_refresh_variant_cache.py`, daarna `python3 scripts/shopify_sync_eta_from_pricelist_csv.py` (zie §3b)

**Alternatief — hele catalogus:** `export_product_ids_and_ymm.py` en `export_product_metafields.py` zonder `--delta-handles-csv` (zie `docs/metafields_manager_export.md`).

**Filter zonder opnieuw te genereren:** `python3 scripts/export_delta_app_imports.py` → o.a. `ymm_APP_import_delta_latest.csv`, `product_metafields_delta_latest.csv`.

---

## 0. Voorbereiding

- Terminal in projectroot; commando’s zonder inline `#` op dezelfde regel.
- Eenmalig: `.env.example` → `.env`, vul `SHOPIFY_ACCESS_TOKEN` in (`docs/shopify_env.md`).

---

## 1. Bronbestanden ophalen (FTP/FTPS)

```bash
python3 scripts/fetch_input_sftp.py
```

Download naar `downloads/ftp/`. Configuratie in `.env` (o.a. `KTM_TRANSFER_PROTOCOL`, `KTM_SFTP_HOST`, `KTM_SFTP_USER`, …). Handig: `--list`, `--dry-run`.

---

## 2. Staging → `input/`

```bash
python3 scripts/prepare_input_from_ftp.py
# XML in zips:
python3 scripts/prepare_input_from_ftp.py --extract-xml-from-zips
```

Opties: `--dry-run`, `--move`, `--files "a.zip,b.csv"`.

---

## 3. Input controleren

- XML (default): `input/CBEXPDN_KTM-DN*.xml` — één match, of bij meerdere het nieuwste bestand; override: `KTM_XML_FILE` in `.env` (zie `.env.example`)
- Prijs-CSV: `input/*0150*.csv`
- Optioneel: Product-Ids fallback-CSV, `input/handle-overrides.json`
- Afbeeldingen: onder `input/` (recursief op bestandsnaam)

---

## 3b. Shopify: variant-cache + KTM prijs-CSV (ETA)

De prijs-CSV’s in `input/` (o.a. 0140/0910/0150/1100) horen bij dezelfde bron als `pricing_loader`; API-scripts (`shopify_sync_*.py`) muteren Shopify rechtstreeks.

**SKU → variant-id-cache** (`cache/shopify_eta_sync_sku_variant.json`):

- Opbouwen/verversen: `python3 scripts/shopify_refresh_variant_cache.py`
- Na **nieuwe** productimport in Shopify opnieuw draaien, anders ontbreken nieuwe SKU’s.

**ETA-sync:** `python3 scripts/shopify_sync_eta_from_pricelist_csv.py` (optioneel `--dry-run`). Namespace/metafield: zie `.env` en `docs/shopify_env.md`.

---

## 4. Product-CSV (`main.py`)

```bash
python3 -u main.py
```

Output o.a.: `output/products/shopify_export_delta_<timestamp>.csv`, `shopify_export_all_<timestamp>.csv`.

---

## 5–6. YMM, product-ID’s en metafields

Uitgebreide commando’s, delta vs. volledig, `export_delta_app_imports.py`: **`docs/metafields_manager_export.md`**.

---

## 7. Controle en validatie

- Snelle YMM-check: `python3 scripts/check_ymm_sku.py <SKU>`
- Outputs: `output/products/`, `output/ids/`, `output/ymm/`, `output/metafields/`
- Exportstijl: o.a. `fits_on`-gerelateerde kolommen in hoofdletters waar afgesproken

---

## 8. Upload / import

- **Shopify:** Admin → Import; meestal delta-CSV; ALL alleen bewust.
- **YMM-app:** delta- of `ALL_part_*.csv` in volgorde, nadat producten in Shopify staan.
- **Metafields Manager:** delta- of volledige CSV uit `output/metafields/`.

---

## 9. Veelvoorkomende problemen

| Probleem | Aanpak |
|----------|--------|
| `0150 prijsbestand niet gevonden` | `*0150*.csv` in `input/` |
| Weinig of geen `fits_on` | Juiste XML; opnieuw export + metafields-stappen |
| Missende images | Bestanden echt onder `input/` |
| API/cache | `--refresh-shopify-cache` bij export-scripts |
| ETA-sync slaat SKU’s over | `shopify_refresh_variant_cache.py` opnieuw |
| Time-outs YMM/metafields | Delta-flow met `--delta-handles-csv` |

---

## 10. Snelle dagelijkse checklist

1. `fetch_input_sftp.py` → `prepare_input_from_ftp.py --extract-xml-from-zips`
2. `main.py`
3. Shopify: delta-CSV importeren (wachten)
4. `export_product_ids_and_ymm.py` + `export_product_metafields.py` met jouw delta-pad
5. Outputs controleren; YMM + metafields uploaden
6. Optioneel §3b: variant-cache → `shopify_sync_eta_from_pricelist_csv.py`

Eerste keer / volledige resync: zonder delta-flags; zie `docs/metafields_manager_export.md`.
