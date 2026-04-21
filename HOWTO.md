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

## SKU controleren (all/delta-export)

Één SKU: staat die in de all-/delta-export en zo niet, waarom (zelfde regels en teksten als `shopify_export_excluded_*.csv`). Standaard geen netwerk (snel); optioneel `--network` voor CDN/Shopify-afbeeldinglookup zoals `main.py` bij lege cache.

```bash
python3 scripts/sku_export_status.py A62612995001
python3 scripts/sku_export_status.py A62612995001 --network
```

Zie [`scripts/sku_export_status.py`](scripts/sku_export_status.py).

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

## Sync prijzen / ETA / draft-status (KTM prijs-CSV → Shopify API)

Prijzen, ETA-datum en publicatiestatus gaan via **`shopify_sync_from_pricelist_csv.py`** (niet via `main.py`-CSV).

Belangrijk voor policy/ETA:
- `inventory_policy` volgt hybride bronregel: **DENY** bij `ArticleStatus=80` of `StockAvailable=0`; anders **CONTINUE** bij `StockAvailable=1/2` of niet-80 status.
- ETA wordt alleen zichtbaar gehouden wanneer Shopify-voorraad niet positief is; bij `inventoryQuantity > 0` wordt ETA gewist.

**Eén keer (of na nieuwe producten in Shopify, zodat SKU’s → variant-id’s kloppen):**

```bash
python3 scripts/shopify_refresh_variant_cache.py
```

**Daarna sync:**

```bash
python3 scripts/shopify_sync_from_pricelist_csv.py
```

Opties o.a.: `--dry-run`, `--csv pad/naar/prijs-export.csv` (meerdere `--csv` voor merge) — zie docstring in het script.

*Alleen* ETA via apart script (als je die flow gebruikt): `scripts/shopify_sync_eta_from_pricelist_csv.py` — zie [`docs/workflow.md`](docs/workflow.md) §3b.

---

## Dubbele variant-SKU’s met x-handle (Shopify API)

Rapportage voor **geïmporteerde dubbele producten**: een SKU komt op **meerdere producten** voor, waarbij minstens één product een **handle op `x`** heeft met **precies één** variant (familie-artikel, bv. `3ki23004580x` naast `3KI230045800`). Producten met alleen een x-handle maar **meerdere** varianten (bv. `3pw24000500x`) kwalificeren niet als anker.

**Uitvoer:** CSV op **stdout** (kolom `row_kind`: `x_single_variant` vs `shared_sku_peer`); voortgang en tellingen op **stderr**. Redirect: `> bestand.csv`.

```bash
python3 scripts/shopify_list_single_variant_sku_suffix_x.py > output/logs/duplicate_x_sku_peers.csv
```

Optioneel: `--active-only` (alleen ACTIVE), `--handle-suffix` (default `x`), `--rest` (REST i.p.v. bulk — kleine shops/debug).

Vereist: `SHOPIFY_ACCESS_TOKEN` / `SHOPIFY_SHOP_DOMAIN` in `.env`. Zie docstring in [`scripts/shopify_list_single_variant_sku_suffix_x.py`](scripts/shopify_list_single_variant_sku_suffix_x.py).

**Zelfde producten op DRAFT zetten** (REST; standaard dry-run, `--apply` voor echt wijzigen). Leest `product_id_numeric` uit de CSV; optioneel `--only-row-kind x_single_variant` als je alleen de x-ankers wilt (niet de `shared_sku_peer`-rij). Of **handles** (URL-slug): `--handles "a,b"` of `--handles-file` met één handle per regel.

```bash
python3 scripts/shopify_set_products_draft.py --csv output/logs/duplicate_x_sku_peers.csv
python3 scripts/shopify_set_products_draft.py --csv output/logs/duplicate_x_sku_peers.csv --only-row-kind x_single_variant --apply
python3 scripts/shopify_set_products_draft.py --handles-file handles.txt --apply
```

Zie [`scripts/shopify_set_products_draft.py`](scripts/shopify_set_products_draft.py).

---

## Ontbrekende productafbeeldingen (`shopify_export_all` → Shopify API)

Twee stappen: **(1) vergelijken** (export + live shop, rapport + JSON), **(2) ontbrekende URL’s koppelen** aan producten. Zelfde URL’s als in de CSV; alleen handles uit de export worden opgehaald. Stap 1 gebruikt standaard **GraphQL** (`handle:a OR handle:b …` in batches) — veel minder API-rondes dan één REST-call per handle; ontbrekende handles daarna via REST. Geen `KTM_SKIP_SHOPIFY_API=1`.

**Stap 1 — vergelijken** (standaard nieuwste `shopify_export_all_*.csv`; schrijft `output/logs/shopify_missing_image_tasks.json` als er ontbrekende images zijn):

```bash
python3 scripts/shopify_compare_export_images.py
python3 scripts/shopify_compare_export_images.py --fetch-workers 16 --graphql-batch 30
# alleen als je de oude trage modus wilt (één REST GET per handle):
python3 scripts/shopify_compare_export_images.py --rest-only --workers 12
```

Alleen rapport, geen JSON: `--no-tasks-file`.

**Stap 2 — koppelen in Shopify** (leest het JSON van stap 1; parallelle POST’s):

```bash
python3 scripts/shopify_apply_missing_images.py
python3 scripts/shopify_apply_missing_images.py --apply-workers 12
```

Alleen tellen, geen wijzigingen: `python3 scripts/shopify_apply_missing_images.py --dry-run`

Zie de docstrings in [`scripts/shopify_compare_export_images.py`](scripts/shopify_compare_export_images.py) en [`scripts/shopify_apply_missing_images.py`](scripts/shopify_apply_missing_images.py).

---

## Vaak samen: bron ophalen

```bash
python3 scripts/fetch_input_sftp.py
python3 scripts/prepare_input_from_ftp.py --extract-xml-from-zips
```

---

## Git push vanuit Cursor (GitHub)

Als push weigert vanwege **`workflow` scope**: je gebruikt HTTPS met een token zonder workflow-recht. **Structurele fix:** SSH voor `origin` — stap-voor-stap in [`docs/git_cursor_github.md`](docs/git_cursor_github.md).

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
