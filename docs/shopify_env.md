# Shopify-gegevens en `.env`

Tokens, shop-URL en CDN-pad staan **niet** meer hardcoded in de Python-code. Ze worden gelezen uit:

1. **Bestaande omgevingsvariabelen** (shell of CI), of
2. **Projectroot `.env`** (eenvoudige `KEY=value`-syntax, geen extra dependency).

## Setup

```bash
cp .env.example .env
# Bewerk .env in je editor en zet SHOPIFY_ACCESS_TOKEN (en andere waarden indien nodig).
```

- **`.env`** staat in `.gitignore` — commit die niet.
- **`.env.example`** is wél in git; alleen placeholders.

## Variabelen

| Variabele | Verplicht | Beschrijving |
|-----------|-----------|--------------|
| `SHOPIFY_ACCESS_TOKEN` | Ja voor API | Admin API access token (`shpat_...`). |
| `SHOPIFY_SHOP_DOMAIN` | Nee | Default `ktm-shop-nl.myshopify.com`. |
| `SHOPIFY_SHOP_SLUG` | Nee | Subdomein vóór `.myshopify.com` (GraphQL-host). |
| `SHOPIFY_ADMIN_API_VERSION` | Nee | Default `2024-10`. |
| `SHOPIFY_CDN_FILES_BASE_URL` | Nee | Basis-URL voor `Image Src` in `exporter`; moet eindigen op `/`. |

Als `SHOPIFY_ACCESS_TOKEN` leeg is, falen API-calls; zet het token in `.env` of exporteer het in je shell.

## Volgorde

- `config.py` laadt `.env` bij import.
- `main.py` en `scripts/*` die `import config` doen, laden eerst `.env` vóór Shopify-modules.

## Alternatieven

- **Alleen shell:** `export SHOPIFY_ACCESS_TOKEN='...'` voordat je `python3 main.py` draait (overschrijft `.env`).
- **macOS Keychain / 1Password CLI:** geen ingebouwde ondersteuning; exporteer naar env of zet in `.env` lokaal.
