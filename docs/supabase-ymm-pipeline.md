# Supabase YMM Pipeline

## Architectuur (twee sporen)

| Spoor | Bron | Supabase | Doel |
|-------|------|----------|------|
| **Catalogus** | Shopify API | `shopify_products`, `shopify_variants`, `shopify_eta` | Prijzen, ETA, customs, staging, SKU→product_id |
| **Fitment (YMM)** | CBEXPDN XML | `canonical_product_fits_on` | Waarheid voor `fits_on`; push naar Shopify |

De **nachtelijke catalog mirror** (`shopify_catalog_mirror` in `job-worker.yml`) blijft voor producten/varianten/prijzen/ETA.

**Standaard vult de mirror geen YMM meer** (`shopify_ymm` uit Shopify is uit). Optioneel legacy: `SHOPIFY_MIRROR_SYNC_FITS_ON=1`.

**Belangrijk:** de mirror **upsert** alleen live producten uit Shopify; ze **verwijdert geen** oude `shopify_products` / `shopify_variants`. Producten die uit Shopify zijn verdwenen blijven in Supabase staan met oude `synced_at` → zie [Spook-producten opschonen](#spook-producten-opschonen).

Migraties in Supabase SQL Editor:

- **`028_canonical_product_fits_on.sql`** — canonical tabel + projection-bron
- **`031_canonical_ymm_summary.sql`** — kolom `ymm_summary` + projection gebruikt Python-summary

`ymm_summary` wordt bij XML-sync in Python berekend (per OEM rijk, bij cross-brand met ` | `) en naar Shopify gepusht als metafield.

---

## `ymm_summary` (Python)

Logica in `modules/metafields_manager_export.py`:

- **KTM:** cc vóór lijn (`125 SX`, `790 ADVENTURE` → STREET)
- **Husqvarna / GASGAS:** cc na lijn (`FE 450`, `FC 250`, `TX 300` → tags FE, FC, TX, …)
- **Cross-brand:** per merk een rijke regel, join met ` | `, bv.  
  `GASGAS 125-300 (TX) 2017-2025 | HUSQVARNA 50-501 (FC, TC, FE, TE) 2014-2027 | KTM 50-1290 (EXC, SX, …) 2014-2027`
- **Eén bouwjaar:** `2009` i.p.v. `2009-2009`

---

## Diff vs full push

| Modus | Wanneer | Commando-flag |
|-------|---------|----------------|
| **Diff** | `content_hash <> pushed_hash` of nog nooit gepusht | `--push-only --write` (default `only_diff=true`) |
| **Full** | Alles opnieuw naar Shopify (bv. na parser-fix terwijl `ymm_json` gelijk bleef) | `--push-only --write --full-push` |

`content_hash` is een hash van **`ymm_json` alleen**, niet van `ymm_summary`. Na een wijziging alleen in de summary-logica: **`--full-push`** nodig, of diff blijft `te_pushen = 0`.

Controleren hoeveel openstaat:

```sql
SELECT count(*) AS te_pushen
FROM canonical_product_fits_on
WHERE ymm_json IS NOT NULL
  AND (pushed_at IS NULL OR content_hash IS DISTINCT FROM pushed_hash);
```

---

## Standaard workflow (lokaal)

### 0) Vereisten

- Migraties **028** + **031** toegepast
- `.env` / `converter/.env.local`: `SUPABASE_*`, `SHOPIFY_*`
- CBEXPDN XML in `input/` (voor sync)

### 1) Catalog mirror (na grote wijzigingen in Shopify of vóór eerste push)

```bash
python3 scripts/queue_supabase_job.py shopify_catalog_mirror --trigger manual
python3 scripts/supabase_job_worker.py
# herhaal worker tot: Geen queued jobs.
```

Of GitHub: **Job worker** → Run workflow.

### 2) XML → canonical + projection

```bash
python3 scripts/run_canonical_ymm_pipeline.py --sync-only --write
```

Met testhandles:

```bash
python3 scripts/run_canonical_ymm_pipeline.py \
  --handles wp-a54029994500,hsq-a54029994500,a54029994500 \
  --sync-only --write
```

### 3) Push naar Shopify (diff)

```bash
# test
python3 scripts/run_canonical_ymm_pipeline.py --push-only --write --limit 10

# productie
python3 scripts/run_canonical_ymm_pipeline.py --push-only --write
```

### Alles in één commando

```bash
python3 scripts/run_canonical_ymm_pipeline.py --write
# = sync + projection + diff push
```

---

## Spook-producten opschonen

**Symptoom:** push faalt met `Owner does not exist`; product niet in Shopify Admin; in Supabase nog `shopify_products` met oude `synced_at` (bv. 2024) terwijl `max(synced_at)` na mirror **vandaag** is.

**Oorzaak:** ID in `canonical_product_fits_on` verwijst naar een product dat Shopify niet meer kent. Mirror verwijdert die rijen niet automatisch.

### Diagnose

```sql
-- wanneer was de laatste mirror?
SELECT max(synced_at) AS laatste_mirror FROM shopify_products;

-- spoken (niet meegenomen in recente mirror)
SELECT count(*) AS spook_producten
FROM shopify_products
WHERE synced_at < '2026-05-21T00:00:00+00';  -- datum aanpassen: vóór laatste mirror-dag

-- canonical op spoken
SELECT count(*) AS canonical_op_spoken
FROM canonical_product_fits_on c
JOIN shopify_products p ON p.shopify_product_id = c.shopify_product_id
WHERE p.synced_at < '2026-05-21T00:00:00+00';
```

Vervang de datum door de dag **vóór** je mirror-run (of gebruik dynamisch):

```sql
WHERE p.synced_at < (SELECT max(synced_at) - interval '2 hours' FROM shopify_products)
```

### Opschonen (volgorde)

**A — canonical (spoken YMM weg)**

```sql
DELETE FROM canonical_product_fits_on c
USING shopify_products p
WHERE p.shopify_product_id = c.shopify_product_id
  AND p.synced_at < '2026-05-21T00:00:00+00';
```

**B — varianten (voorkomt dat sync spoken terugzet)**

```sql
DELETE FROM shopify_variants v
USING shopify_products p
WHERE p.shopify_product_id = v.shopify_product_id
  AND p.synced_at < '2026-05-21T00:00:00+00';
```

**C — opnieuw sync + push**

```bash
python3 scripts/run_canonical_ymm_pipeline.py --sync-only --write
python3 scripts/run_canonical_ymm_pipeline.py --push-only --write
```

Optioneel later: oude `shopify_products`-spoken handmatig opruimen (niet verplicht voor YMM-push).

---

## GitHub Actions

| Workflow | Wat | Wanneer |
|----------|-----|---------|
| **Job worker** | `shopify_catalog_mirror` | Nachtelijk / handmatig — catalogus spiegelen |
| **YMM delivery** | `shopify_ymm_push_diff_from_supabase` | Wekelijks diff-push (limit default 500) |
| **YMM push to Shopify** | sync +/of push, optioneel `full_push` | Handmatig |

### YMM delivery (alleen diff-push)

**Actions → YMM delivery (Supabase → Shopify)**

| Input | Waarde |
|-------|--------|
| `handles` | leeg = alle pending diffs |
| `dry_run` | `false` |
| `limit` | `500` of `0` |

≈ `--push-only --write` (geen XML-sync).

### YMM push to Shopify (handmatig)

| Input | Sync | Push | Gebruik |
|-------|------|------|---------|
| `run_xml_sync=true`, `skip_shopify_push=true` | ja | nee | XML → Supabase (XML moet op runner in `input/`) |
| `run_xml_sync=false`, `full_push=false` | nee | diff | Na lokale sync |
| `run_xml_sync=false`, `full_push=true` | nee | alles | Na parser/summary-fix |
| `handles` | leeg = hele catalogus | | |
| `dry_run` | `false` voor productie | | |

Timeout: **6 uur**. Logs van de push-job komen in één keer vrij (shell buffert worker-output); Shopify wordt wel live bijgewerkt.

Secrets: `SUPABASE_*`, `SHOPIFY_*` (zelfde als job worker).

---

## Jobs via queue (alternatief)

```bash
python3 scripts/queue_supabase_job.py canonical_ymm_sync_to_supabase \
  --trigger manual --payload-json '{"dry_run":false}'
python3 scripts/queue_supabase_job.py shopify_ymm_projection_refresh --trigger manual
python3 scripts/queue_supabase_job.py shopify_ymm_push_diff_from_supabase \
  --trigger manual --payload-json '{"dry_run":false,"limit":0}'
python3 scripts/supabase_job_worker.py
```

Full push job:

```bash
python3 scripts/queue_supabase_job.py shopify_ymm_push_from_supabase \
  --trigger manual --payload-json '{"dry_run":false,"only_diff":false}'
```

---

## Shopify-limieten bij push

- **`fits_on` JSON:** volledige fitment (geen 128-item list-limiet).
- **List-metafields** (`*_new`): max 128 → ingekort; volledige set blijft in JSON.
- **Platte `||`-velden:** ingekort bij zeer lange tekst.
- Bij fout op één product: overige producten gaan door; job faalt als `failed_products > 0`.

---

## Fouten

| Fout | Oorzaak | Actie |
|------|---------|--------|
| `Owner does not exist` | `shopify_product_id` niet in live Shopify | [Spook-producten opschonen](#spook-producten-opschonen) |
| `523` / Supabase read failed | Tijdelijk overload na mirror/sync | Even wachten, opnieuw proberen |
| `te_pushen = 0` maar oude summary in Shopify | `content_hash` alleen op JSON | `--full-push` |
| `Value has more than 128 elements` | Te lange lijst-metafield | Volledige data staat in `fits_on` JSON; lists worden ingekort |

---

## CSV-backup (handmatig)

```bash
python3 scripts/export_product_metafields.py --brand ktm|hsq|wp
```

Zie [`metafields_manager_export.md`](metafields_manager_export.md).

---

## Legacy

| Job | Gebruik |
|-----|---------|
| `shopify_ymm_backfill_from_supabase` | Alleen **lege** Shopify-velden vullen (geen overwrite) |
| `shopify_catalog_mirror` + `SHOPIFY_MIRROR_SYNC_FITS_ON=1` | Oude spiegel van Shopify fits_on → `shopify_ymm` |

Niet combineren met canonical sync op dezelfde tabel.
