# Nieuwe producten — stappenplan

**Automatisch (voorkeur):** dagelijks via VPS + GitHub Actions — zie [`vps-ftp-en-runner.md`](vps-ftp-en-runner.md).  
Prijs/ETA blijft **apart** via Actions (`ktm_price_eta_status_sync`).

Handmatig (laptop) alleen als noodgreep:

```bash
cd ~/Documents/ktm_project
```

---

## 1. Data ophalen (FTP)

Prijs-CSV’s (nacht/uur GHA) of PIM-zips (dagelijks op VPS):

```bash
python3 scripts/fetch_input_sftp.py
python3 scripts/fetch_input_sftp.py --files "XML_Export_PIM_KTM_International.zip,XML_Export_PIM_HQV_International.zip,XML_Export_PIM_WP_International.zip"
```

Of alles-in-één op de VPS:

```bash
python3 scripts/vps_pim_refresh_weekly.py
```

---

## 2. Bestanden klaarzetten

XML + PHO uit PIM-zips (zip gaat daarna weg; oude CBEXPDN weg met `--prune-old-xml`):

```bash
python3 scripts/prepare_input_from_ftp.py
python3 scripts/prepare_input_from_ftp.py --files XML_Export_PIM_KTM_International.zip --extract-xml-from-zips --extract-media-from-zips --prune-old-xml
python3 scripts/prepare_input_from_ftp.py --files XML_Export_PIM_HQV_International.zip --extract-xml-from-zips --extract-media-from-zips --prune-old-xml --input-dir input/hsq
python3 scripts/prepare_input_from_ftp.py --files XML_Export_PIM_WP_International.zip --extract-xml-from-zips --extract-media-from-zips --prune-old-xml --input-dir input/wp
```

---

## 3. Delta-CSV’s maken

```bash
python3 -u scripts/rebuild_shopify_deltas.py
```

| Merk | Bestand |
|------|---------|
| KTM | `output/products/shopify_export_delta_….csv` |
| HSQ | `output/hsq/products/shopify_export_delta_….csv` |
| WP | `output/wp/products/shopify_export_delta_….csv` |

Alleen producten met een werkende foto (<20 MB) staan in de delta.

---

## 4. Producten in Shopify (API)

Geen Admin CSV-import meer:

```bash
python3 scripts/shopify_import_delta_products.py --brands ktm,hsq,wp
# dry-run:  python3 scripts/shopify_import_delta_products.py --dry-run --limit 10
```

Bestaande handles worden overgeslagen.

Ontbrekende foto’s op bestaande producten (na PIM/rebuild):

```bash
python3 scripts/shopify_sync_missing_product_images.py --csv output/products/shopify_export_delta_….csv
```

(In de daily job automatisch voor ktm/hsq/wp.)

---

## 5. Fitment (YMM-metafields)

```bash
python3 scripts/queue_supabase_job.py shopify_catalog_mirror --trigger manual
python3 scripts/supabase_job_worker.py
python3 scripts/run_canonical_ymm_pipeline.py --sync-only --write
python3 scripts/run_canonical_ymm_pipeline.py --push-only --write
python3 scripts/ymm_ensure_updatable.py --write --push --backfill --limit 500
```

---

## 6. Bijbehorende producten (complementary / ET)

```bash
python3 scripts/shopify_sync_empfart_complementary.py --force-refresh-cache --force-write
```

---

## Alles-in-één (VPS / daily job)

```bash
python3 scripts/run_new_products_daily.py
```

GitHub: workflow **ktm_new_products_daily** (SSH naar VPS).

---

## Klaar

De nacht vult verder aan: prijzen/ETA, barcodes, categorieën (alleen als leeg), SEO, publiceren.

Meer detail: [`workflow.md`](workflow.md), [`vps-ftp-en-runner.md`](vps-ftp-en-runner.md), [`supabase-ymm-pipeline.md`](supabase-ymm-pipeline.md).
