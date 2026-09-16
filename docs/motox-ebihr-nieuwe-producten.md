# Motox e-bihr — nieuwe producten + handmatige YMM/metafields

**Doelshop:** Motox (`ktm-shop-nederland`, credentials in `.env.motox`)  
**Bron:** CSV’s in [`motox/e-bihr/`](../motox/e-bihr/) (valid-products, ymm-filter, ymm-metafields)

Dit is een **semi-handmatige** flow: scripts maken gefilterde importbestanden; jij importeert in Shopify Admin, de YMM-app en Metafields Manager.

Voor **automatische sync** (API → Motox Shopify, prijzen + metafields): zie [`docs/motox-ebihr-pipeline.md`](motox-ebihr-pipeline.md) en `scripts/motox_ebihr_sync.py` / workflow `motox_ebihr_sync`.

---

## Voorwaarden

```bash
cd ~/Documents/ktm_project
# .env.motox met SHOPIFY_ACCESS_TOKEN + SHOPIFY_SHOP_DOMAIN
```

Bronbestanden (voorbeeld):

- `motox/e-bihr/valid-products-ebihr-*_chunks/*.csv`
- `motox/e-bihr/ymm-filter-data-ebihr-*.csv`
- `motox/e-bihr/ymm-metafields-data-ebihr-*_chunks/*.csv`

---

## Stappenplan

### 1. Motox-catalogus cachen (SKU + handle + product-IDs)

```bash
python3 scripts/motox_ebihr_refresh_cache.py
```

Schrijft naar `cache/motox/`:

- `shopify_skus.json`
- `shopify_products_index.json`
- `shopify_sku_to_product_id.json`
- `shopify_handle_to_product_id.json`

Status zonder netwerk:

```bash
python3 scripts/motox_ebihr_refresh_cache.py --status
```

### 2. Alleen nieuwe producten filteren

```bash
python3 scripts/motox_ebihr_filter_new_products.py
```

**Uitsluitregel:** product valt af als de **Handle** of **minstens één Variant SKU** al op Motox staat, of als er **geen Image Src** in de bron-CSV staat.

Als de bron-CSV geen **Title** op de eerste productrij heeft, vult het script die aan (`Vendor Type (handle)`), anders weigert Shopify de import.

Output in `motox/e-bihr/output/products/`:

| Bestand | Inhoud |
|---------|--------|
| `new_only_part_NNN.csv` | Shopify product-import chunks (≤ ~14 MB) |
| `new_only_handles.json` | Lijst new-only handles (voor stap 4) |
| `new_only_skus.json` | Lijst new-only SKUs |
| `report_new_only.csv` | Per handle: `new_only` / `skip` + reden |

Optioneel cache + filter in één keer:

```bash
python3 scripts/motox_ebihr_filter_new_products.py --refresh-cache
```

### 3. Handmatig: producten importeren in Motox

1. Shopify Admin Motox → **Products → Import**
2. Importeer `new_only_part_001.csv`, daarna `002`, … **achter elkaar**
3. Wacht tot elke import **klaar** is voordat je de volgende start
4. Controleer steekproefsgewijs een paar handles in de admin

Image-URL’s wijzen naar `api.mybihr.com`; Shopify moet die bij import kunnen ophalen.

### 4. YMM + metafields voor new-only (na import)

Cache **opnieuw** verversen zodat nieuwe Shopify product-IDs erin staan, daarna exports bouwen:

```bash
python3 scripts/motox_ebihr_prepare_ymm_metafields.py --refresh-cache
```

Output:

| Map / bestand | Gebruik |
|---------------|---------|
| `motox/e-bihr/output/ymm/ymm_APP_import_new_only*.csv` | **YMM-app** — kolom `Product Ids` = Shopify product-ID; 1 regel per product + Make + Model + Year |
| `motox/e-bihr/output/metafields/product_metafields_new_only*.csv` | **Metafields Manager** — o.a. `id`, `handle`, `fits_on`, `ymm_summary` |
| `…/report_unmatched_tokens.csv` | Tokens uit de bron zonder match op Motox |

Exit code `2` = nog geen gemapte producten (meestal: import nog niet klaar of cache niet ververst).

### 5. Handmatig: apps

1. **YMM-app:** importeer de `ymm_APP_import_new_only*.csv` delen achter elkaar  
2. **Metafields Manager:** importeer `product_metafields_new_only*.csv` delen achter elkaar  

Gebruik **niet** de ruwe `ymm-filter-data-ebihr-*.csv` (tilde-artikelnummers) of alleen-`handle,fits_on` metafields-bestanden voor Motox — die koppelen niet op Shopify product-IDs / theme-velden.

---

## Waarom deze volgorde?

De e-bihr YMM-bron is voertuig-gecentreerd (`Product Ids` = Bihr-artikelnummers met `~`). De Motox YMM-app verwacht **Shopify product-IDs**. Die bestaan pas **nadat** de producten geïmporteerd zijn; daarom eerst producten, dan cache refresh, dan YMM/metafields.

---

## Scripts

| Script | Rol |
|--------|-----|
| [`scripts/motox_ebihr_refresh_cache.py`](../scripts/motox_ebihr_refresh_cache.py) | Motox-index ophalen |
| [`scripts/motox_ebihr_filter_new_products.py`](../scripts/motox_ebihr_filter_new_products.py) | New-only product-CSV’s |
| [`scripts/motox_ebihr_prepare_ymm_metafields.py`](../scripts/motox_ebihr_prepare_ymm_metafields.py) | YMM + metafields met Shopify-IDs |

Gedeelde cache-logica: [`modules/motox_shopify_cache.py`](../modules/motox_shopify_cache.py).
