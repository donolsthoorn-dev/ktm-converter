# ZBH2BIKE → YMM / `fits_on`

## Probleem

Veel accessoires (o.a. plastic kits) hangen in de structuurboom alleen aan een pseudo-`$M-<sku>`-node. De **brede** fitment (welke motor / jaren) staat in de XML vooral **omgekeerd**: op elk **complete motor-PRODUKT** (`KLASSE` `$KL-ARTICLE_BIKES`) staat onder `BEZIEHUNGEN` → `BEZIEHUNGSTYP name="ZBH2BIKE"` een lange lijst van onderdeelnummers die bij die motor horen.

## Oplossing in code

1. **Eerste pass** (ongewijzigd): `STRUKTUR_ELEMENT` + `PRODUKT_ZU_STRUKTUR_ELEMENT` → `collect_sku_to_ymm_from_structure()`.

2. **Tweede pass**: `stream_zbh2bike_part_ymm()` leest alle `PRODUKT`-einden met `lxml.iterparse`. Voor elke **motor** met ZBH2BIKE:
   - YMM van die motor: uit (1) als de motor-SKU ook onder een Bikes-`MODELL` hangt, anders uit **BEZEICHNUNG** + `_parse_year()` (zelfde logica als elders). Als `CULTURE` (bijv. `EN-GB`) geen BEZEICHNUNG heeft, wordt de eerste beschikbare `TEXT` onder `BEZEICHNUNG` gebruikt (vaak `DE-AT` bij motoren).
   - Elke onderdeel-SKU in die lijst krijgt die YMM-tuples **bij** (union met bestaande tuples).

3. **Merge**: `build_merged_sku_to_ymm()` = union van (1) en (2). Gebruikt door:
   - `export_ymm_fitment()` → `ymm_APP_import_ALL.csv`
   - `export_product_metafields_csv()` → `fits_on` JSON

## lxml: geneste `PRODUKT`

`iterparse(..., tag="PRODUKT")` vuurt ook **geneste** `<PRODUKT name="…"/>` onder `BEZIEHUNGSTYP` af vóór de ouder-motor sluit. **`elem.clear()` op zo’n kind verwijdert de boom van de ouder** voordat die verwerkt wordt. Daarom wordt alleen gecleared als het element **niet** direct onder `BEZIEHUNGSTYP` hangt (`_produkt_is_nested_beziehungstyp_ref`).

## Controleren

```bash
python3 -c "
from modules.ymm_export import stream_xml_for_export, build_merged_sku_to_ymm
from config import XML_FILE
s, r = stream_xml_for_export()
m = build_merged_sku_to_ymm(s, r, XML_FILE)
print('00010000318 tuples:', len(m.get('00010000318', set())))
"
```

## Scope

- **Geen wijziging** aan `output/shopify/` product-CSV (`xml_loader` / hoofdexport): alleen rapporten onder `output/reports/` en YMM-logica in `modules/ymm_export.py` + `modules/metafields_manager_export.py`.
