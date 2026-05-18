# Supabase YMM Pipeline

Deze pipeline maakt `shopify_ymm` bruikbaar voor:

- oranje YMM-filter (eigen brondata)
- backfill van ontbrekende Shopify metafields

## 1) SQL migratie

Voer migratie `converter/supabase/migrations/019_shopify_ymm_projection.sql` uit.

Die voegt toe:

- tabel `public.shopify_ymm_projection`
- functie `public.refresh_shopify_ymm_projection()`

De projectie bevat per product o.a.:

- `shopify_product_id`, `sku`
- `fits_on_make_old`, `fits_on_model_old`, `fits_on_year_old` (`||` formaat)
- `fits_on_make_new`, `fits_on_model_new`, `fits_on_year_new` (`jsonb` arrays)
- `ymm_summary`

## 2) Build projectie als job

Queue:

```bash
python3 scripts/queue_supabase_job.py shopify_ymm_projection_refresh --trigger manual
```

Run:

```bash
python3 scripts/supabase_job_worker.py
```

## 3) Backfill Shopify metafields vanuit Supabase

Dry-run:

```bash
python3 scripts/queue_supabase_job.py shopify_ymm_backfill_from_supabase \
  --trigger manual \
  --payload-json '{"dry_run": true, "limit": 10}'
python3 scripts/supabase_job_worker.py
```

Gericht op 1 handle (write):

```bash
python3 scripts/queue_supabase_job.py shopify_ymm_backfill_from_supabase \
  --trigger manual \
  --payload-json '{"dry_run": false, "limit": 1, "only_handles": ["00029920000eb"]}'
python3 scripts/supabase_job_worker.py
```

### Welke metafields worden bijgevuld

Alleen als leeg:

- `global.fits_on_make`
- `global.fits_on_model`
- `global.fits_on_year`
- `custom.fits_on_make_new`
- `custom.fits_on_model_new`
- `custom.fits_on_year_new`
- `global.ymm_summary`

De worker is idempotent en overschrijft bestaande waarden niet.
