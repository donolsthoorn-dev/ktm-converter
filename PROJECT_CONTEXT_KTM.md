# 📦 PROJECT CONTEXT – KTM → Shopify Export

Operationeel stappenplan (FTP → import → apps): **`docs/workflow.md`**. YMM/metafields in detail: **`docs/metafields_manager_export.md`**.

## Doel

Python-project dat:

* Een grote KTM XML feed verwerkt (~238MB)
* Een 0150_... CSV prijslijst verwerkt
* Een volledige Shopify import CSV genereert

---

## Architectuur

Projectstructuur:

```
main.py
modules/
    xml_loader.py
    pricing_loader.py
    exporter.py
config.py
input/
output/
```

---

## Secrets en shop-configuratie

Shopify-token, CDN-URL en shop-naam staan in **`.env`** (niet in git). Kopieer `.env.example` naar `.env` en vul `SHOPIFY_ACCESS_TOKEN` in. Zie **`docs/shopify_env.md`**.

---

## Technische randvoorwaarden

### XML verwerking

* XML is groot → geen volledige etree.parse + root.findall(".//...")
* Gebruik iterparse waar nodig
* Geen dubbele passes tenzij echt noodzakelijk
* Geen elem.clear() die later nog nodig is

---

### Variant-logica

* Varianten worden bepaald via:

  * PRODUKT_ZU_STRUKTUR_ELEMENT relaties
  * SKU → ATTRIBUTE → ATTRIBUT → ATTRIBUTWERT
* Per STRUCTUUR_ELEMENT wordt de beste variant-as gekozen
* Heuristiek via score_candidate()
* Fallback alleen indien geen variërend attribuut

---

### Type & Product Category

* Worden opgebouwd via:

  * STRUCTUUR_ELEMENT
  * PARENT_NAME
  * Grandparent → Product category
  * Parent → Type

---

### Pricing

* 0150_... CSV
* SKU = kolom B
* SalesPrice = kolom E
* GTIN = kolom X
* Price = SalesPrice * 1.21
* Barcode = GTIN
* Encoding fallback: utf-8 / utf-8-sig / cp1252 / latin1

---

### Output

* Volledige Shopify header behouden
* Geen kolommen verwijderen
* Geen header versimpelen
* Variant correct gevuld
* Barcode gevuld
* Price gevuld

---

## Niet doen

* Geen simplificatie van exporter
* Geen verwijderen van bestaande werkende logica
* Geen onverwachte header-wijzigingen
* Geen architectuur-wijzigingen zonder expliciete reden

---

## Huidige status

Systeem werkt voor:

* XML parsing
* Variant detectie
* Type & Category hiërarchie
* Pricing + Barcode koppeling
* Shopify CSV output

---

## Workflow: YMM + metafields

Na `main.py` staat de delta-CSV onder `output/products/shopify_export_delta_<timestamp>.csv`.  
Volgorde, commando’s, delta vs. volledige catalogus en `export_delta_app_imports`: **`docs/workflow.md`** (hoofdlijn) en **`docs/metafields_manager_export.md`** (metafields/YMM-detail).

---

## Ontwikkeling (tests, lint, CI)

* **Python:** ondersteund bereik staat in **`pyproject.toml`** (`requires-python`).
* **Dependencies:** productie `pip install -r requirements.txt`; ontwikkeling `pip install -r requirements-dev.txt` (pytest, ruff).
* **Tests:** `python -m pytest tests/`
* **Lint/format:** `ruff check .` en `ruff format .` (config: `pyproject.toml`).
* **CI:** GitHub Actions (`.github/workflows/ci.yml`) op push/PR: ruff, `compileall`, pytest (Python 3.10 en 3.12).

### Foutopsporing `main.py` (ETL)

* Log: standaard **`output/logs/ktm_etl_<timestamp>.log`** (ook naar stdout); optioneel `KTM_LOG_FILE`, niveau o.a. `KTM_LOG_LEVEL=DEBUG`.
* Zie **`docs/workflow.md`** (sectie *Bij falende ETL / import*) voor checklist (input, `.env`, console).

---

