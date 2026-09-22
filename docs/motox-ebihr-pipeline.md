# e-bihr V3 + VSE pipeline

Eigen pipeline: Bihr **V3 catalogs** + **VSE fitment** → Motox Shopify (API) + optionele CSV’s.

Credentials: [`.env.bihr`](../.env.bihr) / [`.env.motox`](../.env.motox) lokaal; in CI via GitHub Secrets (`BIHR_*`, `SHOPIFY_MOTOX_*`). **Nooit credentials committen.**

## Automatische sync (v1) — aanbevolen

Nachtelijke (of handmatige) job schrijft **direct naar Motox Shopify**:

1. Bihr V3 + VSE ophalen
2. New-only producten **aanmaken** (met images) + publiceren op **alle sales channels**
3. Bestaande producten **zonder afbeelding**: images backfill als Bihr ze nu wel heeft, daarna opnieuw publiceren op alle kanalen
4. **Prijzen (delta):** alleen SKUs waar Bihr-prijs ≠ huidige Motox-prijs (uit cache); `--force-all-prices` voor alles
5. **Metafields** `global.fits_on` (+ summary/year/make/model) op nieuwe producten, en op bestaande producten als de passendheid leeg is of afwijkt van VSE
6. **Douane:** Bihr `CommodityCode` en `CountryOfOrigin` op nieuwe producten meteen, en op bestaande producten als de goederencode of het land van herkomst leeg is of afwijkt

YMM-**app**/filter-CSV is **later** (lokaal); v1 dekt theme-metafields.

Motox app-scopes: products/variants/metafields write; voor alle kanalen ook **`read_publications` + `write_publications`**. Zonder die scopes valt publish terug op alleen Online Store.

```bash
# Lokaal dry-run (geen Shopify-writes)
python3 scripts/motox_ebihr_sync.py --raw-dir motox/e-bihr/raw/<ts> --dry-run --max-create 5

# Lokaal apply
python3 scripts/motox_ebihr_sync.py --fetch --apply

# Alleen prijzen (delta t.o.v. Motox-cache)
python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --prices-only

# Alle prijzen forceren (geen delta)
python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --prices-only --force-all-prices

# Alleen image-backfill + kanalen (begrensd)
python3 scripts/motox_ebihr_sync.py --raw-dir ... --apply --images-only --max-image-backfill 50
```

### GitHub Actions

Workflow: [`.github/workflows/motox_ebihr_sync.yml`](../.github/workflows/motox_ebihr_sync.yml)

| Secret | Verplicht |
|--------|-----------|
| `BIHR_USERNAME` | ja |
| `BIHR_PASSWORD` | ja |
| `SHOPIFY_MOTOX_ACCESS_TOKEN` | ja |
| `SHOPIFY_MOTOX_SHOP_DOMAIN` | ja (`ktm-shop-nederland.myshopify.com`) |
| `SHOPIFY_MOTOX_ADMIN_API_VERSION` | optioneel |
| `BIHR_API_BASE` | optioneel |

- **Schedule:** `0 1 * * *` UTC (~03:00 NL zomer)
- **Manual:** Actions → `motox_ebihr_sync` → eerst `apply=false` (dry-run), daarna `apply=true`
- Artifacts: `motox/e-bihr/output/sync/` + Step Summary

## CSV-pipeline (debug / handmatige import)

Nog beschikbaar:

```bash
python3 scripts/ebihr_fetch.py
python3 scripts/ebihr_generate.py --raw-dir motox/e-bihr/raw/<ts> --motox-ready
```

Zie ook [motox-ebihr-nieuwe-producten.md](motox-ebihr-nieuwe-producten.md).

## Filters

- Geen merken: RST, CAPIT, OXFORD, PINLOCK
- Hardparts: OFFROAD via VSE + allowlist smeermiddel/transport/gereedschap
- LEATT / geen images → draft / skip create
- Handle = eerste 7 cijfers PartNumber

## Modules

| Bestand | Rol |
|---------|-----|
| `modules/ebihr/client.py` | V3 + VSE fetch |
| `modules/ebihr/build_*.py` | Products / YMM |
| `modules/ebihr/motox_publish.py` | Shopify create / prices / metafields |
| `scripts/motox_ebihr_sync.py` | End-to-end sync CLI |
| `scripts/ebihr_fetch.py` / `ebihr_generate.py` | CSV-route |
