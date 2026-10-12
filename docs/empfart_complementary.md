# XML-relaties → Shopify complementary / ET

Bron: CBEXPDN `BEZIEHUNGEN` → `BEZIEHUNGSTYP` in KTM/HSQ/WP XML (`input/`).

| Type | Betekenis | Shopify |
|------|-----------|---------|
| `MUSSARTIKEL` | Verplicht mee | Eerst in `complementary_products` |
| `EMPFART2ART` | Aanbevolen erbij | Daarna in `complementary_products` |
| `ET2ARTIKEL` | Onderdelen/componenten | `global.et_articles` (`list.product_reference`) |

**Complementary metafield (Search & Discovery):**

- namespace: `shopify--discovery--product_recommendation`
- key: `complementary_products`

**ET metafield:**

- namespace/key: `global.et_articles`

**UI:** één grid rechts naast de tabs — complementary (MUSS/EMPF) en ET samengevoegd, dedupe, max 16, 8 per pagina. Kop: “Bijbehorende of bijpassende producten”. Complementary-relaties worden **wederkerig** gemaakt (connected component / clique): als A↔B↔C in EMPF zitten, krijgt elk lid de andere leden.

**Related-products** (“WELLICHT OOK INTERESSANT”) blijft Shopify’s algoritme; EMPF hoort daar niet (al in de side-grid).

```bash
python3 scripts/shopify_sync_empfart_complementary.py --dry-run --sku A62409960044
python3 scripts/shopify_sync_empfart_complementary.py --sku A62409960044
python3 scripts/shopify_sync_empfart_complementary.py --force-refresh-cache --force-write
```

`--force-write` = snellere full sync (geen read-compare per product).  
Na **nieuwe productimport** uit `main.py`/CSV: altijd `--force-refresh-cache` zodat nieuwe SKUs in de mapping zitten (clique-symmetrie vereist ook updates op bestaande owners).

Flags: `--limit N`, `--max-products 10`, `--force-refresh-cache`, `--force-write`, `--skip-et`, `--skip-complementary`.

SKU → product via `cache/shopify_sku_to_product_id.json`. Stale product-IDs worden bij write overgeslagen.

Scope: **ktm-shop.nl** alleen.

Theme: tabs + side-relations op PDP (`.ktm-product-meta`, `snippets/ktm-product-side-relations.liquid`).

Workflow-stap: na Shopify-import van nieuwe producten — zie `docs/workflow_nieuwe_producten.txt` stap 7 en `docs/STAPPENPLAN.md`.
