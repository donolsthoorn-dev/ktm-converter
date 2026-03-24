# 📦 PROJECT CONTEXT – KTM → Shopify Export

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

