# GitHub Actions-namen

De naam in Actions is de bestandsnaam onder `.github/workflows/`. Supabase dispatcht op die bestandsnaam.

- `ktm_` — alleen ktm-shop.nl
- `shops_` — ktm-shop.nl én Motox (ktm-shop-nederland)
- `motox_` — alleen Motox

Python-scripts houden hun bestaande bestandsnaam. Alleen de workflowbestanden zijn hernoemd (migratie `035_rename_github_workflow_files.sql`).

## Hernoemd naar `ktm_`

| Oud | Nieuw |
|-----|-------|
| `customs_missing_fill.yml` | `ktm_customs_missing_fill.yml` |
| `job-worker.yml` (Actions: Job worker) | `ktm_job_worker.yml` |
| `price_eta_status_sync.yml` | `ktm_price_eta_status_sync.yml` |
| `shopify_auto_deactivate_invalid_products.yml` | `ktm_shopify_auto_deactivate_invalid_products.yml` |
| `shopify_gtin_fill.yml` | `ktm_shopify_gtin_fill.yml` |
| `shopify_publish_sellable_active_products.yml` | `ktm_shopify_publish_sellable_active_products.yml` |
| `ymm-delivery-schedule.yml` (Actions: YMM delivery) | `ktm_ymm_delivery.yml` |
| `ymm-push-shopify.yml` (Actions: YMM push to Shopify) | `ktm_ymm_push.yml` |

## Hernoemd naar `shops_`

| Oud | Nieuw | Waarom beide |
|-----|-------|----------------|
| `shopify_category_fill_empty.yml` | `shops_category_fill_empty.yml` | Lege category op ktm-shop.nl en Motox |
| `shopify_category_reclassify.yml` | `shops_category_reclassify.yml` | Herclassificatie op ktm-shop.nl en Motox |
| `motox_synkro_clone_missing.yml` | `shops_synkro_clone_missing.yml` | Tag op KTM, Synkro kloont naar Motox POS |

## Blijft `motox_`

- `motox_ebihr_seo.yml`
- `motox_ebihr_sync.yml`
- `motox_deactivate_products_without_image.yml`
- `motox_delete_handle_dupes.yml` — nachtelijke `-1`-handles, max 100
- `motox_shopify_gtin_fill.yml` — lege barcode uit e-bihr `BarCode`
- `motox_oem_price_sync.yml` — KTM-prijs op ongepubliceerde OEM-producten

Na een push naar `main` toont Actions de nieuwe namen. Bestaande run-historie blijft aan het oude workflowbestand hangen. De Supabase-scheduler pakt de nieuwe namen pas na migratie `035`.
