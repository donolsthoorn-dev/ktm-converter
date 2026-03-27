# How to — snelle commando’s

Alle commando’s vanaf de **projectroot** (`ktm_project/`). Python: `python3` (zie `pyproject.toml`).

Uitgebreide uitleg: [`docs/workflow.md`](docs/workflow.md). Flow alleen nieuwe producten: [`docs/workflow_nieuwe_producten.txt`](docs/workflow_nieuwe_producten.txt).

---

## XML → product-CSV (Shopify-importbestanden)

```bash
python3 -u main.py
```

Output o.a.: `output/products/shopify_export_delta_<timestamp>.csv` en `shopify_export_all_*.csv`.

**Input:** `input/CBEXPDN_KTM-DN*.xml` + `input/*0150*.csv` (+ evt. afbeeldingen). Zie [`config.py`](config.py) / `KTM_XML_FILE` in `.env`.

---

## YMM (app-import)

**Delta** (na import van die producten in Shopify; vervang het delta-pad door jouw bestand):

```bash
python3 -u scripts/export_product_ids_and_ymm.py --refresh-shopify-cache \
  --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
```

**Hele catalogus** (groot; alleen als je dat bewust wilt):

```bash
python3 -u scripts/export_product_ids_and_ymm.py
```

Output o.a.: `output/ymm/ymm_APP_import_*.csv`

---

## Metafields Manager-export

**Delta** (zelfde `--delta-handles-csv` als bij YMM):

```bash
python3 -u scripts/export_product_metafields.py \
  --delta-handles-csv output/products/shopify_export_delta_JJJJMMDD_HHMMSS.csv
```

**Hele catalogus:**

```bash
python3 -u scripts/export_product_metafields.py
```

Output o.a.: `output/metafields/product_metafields_metafields_manager_delta.csv` (delta) of `product_metafields_metafields_manager.csv` (volledig).

---

## Sync prijzen / ETA / draft-status (0150 → Shopify API)

Prijzen, ETA-datum en publicatiestatus gaan via **`shopify_sync_from_0150.py`** (niet via `main.py`-CSV).

**Eén keer (of na nieuwe producten in Shopify, zodat SKU’s → variant-id’s kloppen):**

```bash
python3 scripts/shopify_refresh_variant_cache.py
```

**Daarna sync:**

```bash
python3 scripts/shopify_sync_from_0150.py
```

Opties o.a.: `--dry-run`, `--csv pad/naar/0150.csv` — zie docstring in het script.

*Alleen* ETA via apart script (als je die flow gebruikt): `scripts/shopify_sync_eta_from_0150.py` — zie [`docs/workflow.md`](docs/workflow.md) §3b.

---

## Ontbrekende productafbeeldingen (`shopify_export_all` → Shopify API)

Vergelijkt **Image Src** in een `shopify_export_all_*.csv` met de live shop en voegt ontbrekende afbeeldingen toe (zelfde URL’s als in de CSV). Er worden **alleen producten opgehaald voor handles die in die CSV voorkomen** (niet de hele catalogus); parallel via **`--workers N`** (default 6). Standaard alleen een rapport; **`--apply`** voert de wijzigingen uit. Geen `KTM_SKIP_SHOPIFY_API=1` — dit script heeft live API nodig.

**Dry-run** (default; gebruikt standaard de nieuwste `shopify_export_all_*.csv` in `output/products/`):

```bash
python3 scripts/shopify_sync_images_from_csv.py
```

**Echt bijwerken** (optioneel `--csv` / `--limit N` voor testen):

```bash
python3 scripts/shopify_sync_images_from_csv.py --apply
```

Zie de docstring in [`scripts/shopify_sync_images_from_csv.py`](scripts/shopify_sync_images_from_csv.py) voor alle opties.

---

## Vaak samen: bron ophalen

```bash
python3 scripts/fetch_input_sftp.py
python3 scripts/prepare_input_from_ftp.py --extract-xml-from-zips
```

---

## Handig

Laatste delta-CSV vinden:

```bash
ls -t output/products/shopify_export_delta_*.csv | head -1
```

Alleen app-CSV’s filteren zonder opnieuw te genereren:

```bash
python3 scripts/export_delta_app_imports.py
```
