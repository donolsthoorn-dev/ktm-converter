# Shopify-cache en periodiek draaien

## Wat gebeurt er standaard?

- Bestanden onder `cache/` (`shopify_products_index.json`, `shopify_sku_to_product_id.json`, …) worden **hergebruikt** zodat elke run niet opnieuw de hele Shopify-catalogus hoeft te downloaden.
- **Handmatig** cache wissen is niet nodig als je onderstaande opties gebruikt.

## Automatisch “vers genoeg” houden (TTL)

Zet in je shell of in `~/.zshrc` (voor alle runs in Terminal):

```bash
export KTM_SHOPIFY_CACHE_MAX_AGE_DAYS=7
```

- Als een cache-bestand **ouder is dan 7 dagen**, wordt het bij de **volgende** normale run **automatisch** opnieuw opgehaald.
- Zet de waarde op `0` of laat de variabele weg → **geen** automatische verloopdatum (oud gedrag: cache blijft tot je hem weggooit).

## Eén keer alles forceren (zonder bestanden te zoeken)

```bash
export KTM_FORCE_REFRESH_SHOPIFY_CACHE=1
python3 -u scripts/export_product_ids_and_ymm.py
```

Of alleen voor die run:

```bash
KTM_FORCE_REFRESH_SHOPIFY_CACHE=1 python3 -u scripts/export_product_ids_and_ymm.py
```

Of met vlag:

```bash
python3 -u scripts/export_product_ids_and_ymm.py --refresh-shopify-cache
```

## Volledig periodiek, zonder dat je Terminal open hebt (macOS)

Het script doet **geen** achtergronddaemon; “periodiek uit zichzelf” regel je met **launchd** of **cron**.

### Voorbeeld: elke zondag om 03:00

1. Maak een script `~/bin/ktm_export_weekly.sh`:

```bash
#!/bin/bash
cd /pad/naar/ktm_project || exit 1
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy
export KTM_FORCE_REFRESH_SHOPIFY_CACHE=1
/usr/bin/python3 -u scripts/export_product_ids_and_ymm.py >> output/logs/ymm_export_cron.log 2>&1
```

2. `chmod +x ~/bin/ktm_export_weekly.sh`

3. Voeg in **crontab** toe (`crontab -e`):

```cron
0 3 * * 0 /Users/jouwnaam/bin/ktm_export_weekly.sh
```

*(Pas pad en gebruikersnaam aan.)*

### Alternatief: alleen TTL, geen vaste refresh

Laat `KTM_FORCE_REFRESH_SHOPIFY_CACHE` weg en zet alleen:

```bash
export KTM_SHOPIFY_CACHE_MAX_AGE_DAYS=7
```

in hetzelfde cron-/launchd-script vóór `python3`. Dan wordt de cache alleen vernieuwd als hij **te oud** is.

## Offline / geen API

```bash
export KTM_SKIP_SHOPIFY_API=1
```

Gebruikt alleen bestaande cache; geen netwerk. Handig op een netwerk met proxyproblemen; voor verse data eerst ergens met werkende API draaien.
