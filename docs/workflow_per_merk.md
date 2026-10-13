# Workflow per merk — referentie

**Dagelijks werk:** gebruik **[`STAPPENPLAN.md`](STAPPENPLAN.md)** (stappen 1 t/m 6, alle merken).

Dit bestand is alleen een **naslag** voor paden en bestandsnamen per merk.

---

## Bestanden per merk

| Merk | Input | `main.py` | Delta-CSV | Metafields-output |
|------|-------|-----------|-----------|-------------------|
| **KTM** | `input/` | `python3 -u main.py` | `output/products/shopify_export_delta_*.csv` | `output/metafields/` |
| **HSQ** | `input/hsq/` | `… --brand hsq` | `output/hsq/products/…` | `output/hsq/metafields/` |
| **WP** | `input/wp/` | `… --brand wp` | `output/wp/products/…` | `output/wp/metafields/` |

### XML en prijs-CSV

| Merk | XML | Prijs-CSV |
|------|-----|-----------|
| KTM | `CBEXPDN_KTM-DN*.xml` | `0150_35_Z1_EUR_EN_csv.csv` |
| HSQ | `CBEXPDN*.xml` | `1100_…` + `0140_…` |
| WP | `CBEXPDN*.xml` | `0910_35_Z1_EUR_EN_csv.csv` |

---

## Stappen 4–5 (copy-paste)

Zie [`STAPPENPLAN.md`](STAPPENPLAN.md) §4–5.

---

## Veelvoorkomende fouten

| Probleem | Oplossing |
|----------|-----------|
| `main.py brands hsq` | Gebruik `--brand hsq` |
| Metafields zonder product-id | Shopify-import afwachten; `--refresh-shopify-cache` bij eerste merk in stap 4 |
| `metafields_missing_product_ids.csv` | Producten ontbreken in Shopify; eerst stap 3 voor dat merk |
| Time-out in app | Stap 4 met `--delta-handles-csv` i.p.v. volledige catalogus |

Technische achtergrond: [`workflow.md`](workflow.md), [`metafields_manager_export.md`](metafields_manager_export.md).
