# Supabase YMM Pipeline

## Architectuur (twee sporen)

| Spoor | Bron | Supabase | Doel |
|-------|------|----------|------|
| **Catalogus** | Shopify API | `shopify_products`, `shopify_variants`, `shopify_eta` | Prijzen, ETA, customs, staging, SKU→product_id |
| **Fitment (YMM)** | CBEXPDN XML | `canonical_product_fits_on` | Waarheid voor `fits_on`; push naar Shopify bij diff |

De **nachtelijke catalog mirror** (`shopify_catalog_mirror` in `job-worker.yml`) blijft voor producten/varianten/prijzen/ETA.

**Standaard vult de mirror geen YMM meer** (`shopify_ymm` uit Shopify is uit). Optioneel legacy: `SHOPIFY_MIRROR_SYNC_FITS_ON=1`.

Migratie **`028_canonical_product_fits_on.sql`** uitvoeren in Supabase SQL Editor.

---

## 1) XML → canonical (na nieuwe CBEXPDN)

```bash
python3 scripts/queue_supabase_job.py canonical_ymm_sync_to_supabase \
  --trigger manual \
  --payload-json '{"dry_run":false,"only_handles":["wp-a54029994500"]}'
python3 scripts/supabase_job_worker.py
```

Of lokaal:

```bash
python3 scripts/run_canonical_ymm_pipeline.py --handles wp-a54029994500 --sync-only --write
```

Projection verversen:

```bash
python3 scripts/queue_supabase_job.py shopify_ymm_projection_refresh --trigger manual
```

`refresh_shopify_ymm_projection()` leest nu **`canonical_product_fits_on`**, niet `shopify_ymm`.

---

## 2) Diff-push naar Shopify

Alleen producten waar `content_hash <> pushed_hash` (of nog nooit gepusht).

```bash
python3 scripts/run_canonical_ymm_pipeline.py \
  --handles wp-a54029994500,hsq-a54029994500,a54029994500 \
  --push-only --write
```

Job:

```bash
python3 scripts/queue_supabase_job.py shopify_ymm_push_diff_from_supabase \
  --trigger manual \
  --payload-json '{"dry_run":false,"limit":500}'
```

**GitHub:** workflow **YMM delivery (Supabase → Shopify)** — wekelijks + handmatig (`ymm-delivery-schedule.yml`).

---

## 3) Volledige pipeline (test / fix)

```bash
python3 scripts/run_canonical_ymm_pipeline.py \
  --handles wp-a54029994500,hsq-a54029994500,a54029994500 \
  --write
```

Stappen: sync canonical → projection refresh → diff push.

---

## CSV-backup (handmatig)

Blijft beschikbaar:

```bash
python3 scripts/export_product_metafields.py --brand ktm|hsq|wp
```

---

## Shopify-limieten bij push

- **`fits_on` JSON:** volledige fitment (geen 128-item list-limiet).
- **List-metafields** (`*_new`): max 128 → ingekort; volledige data in JSON.
- **Platte `||`-velden:** ingekort bij zeer lange tekst.

---

## Legacy

| Job | Gebruik |
|-----|---------|
| `shopify_ymm_backfill_from_supabase` | Alleen **lege** Shopify-velden vullen (geen overwrite) |
| `shopify_catalog_mirror` + `SHOPIFY_MIRROR_SYNC_FITS_ON=1` | Oude spiegel van Shopify fits_on → `shopify_ymm` |

Niet combineren met canonical sync op dezelfde tabel.

---

## GitHub Actions

| Workflow | Wat |
|----------|-----|
| **Job worker** (nacht) | `shopify_catalog_mirror` — producten/varianten/ETA |
| **YMM delivery** (week) | `shopify_ymm_push_diff_from_supabase` |
| **YMM push to Shopify** (handmatig) | sync + push voor testhandles |

Vereist migratie **019**, **020**, **028** in Supabase.
