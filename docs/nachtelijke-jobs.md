# Automatische jobs

Wat er elke dag vanzelf gebeurt.

## ktm-shop.nl

### 22:45 — SEO teksten

Schrijft de titel, de korte Google-tekst en de alt-tekst van de foto. Eerst producten die te koop zijn en op voorraad liggen, daarna de rest. Maximaal 4000 per nacht.

Alleen lege teksten worden ingevuld. Wat al geschreven is, blijft staan. Artikelnummer en barcode worden niet aangepast.

### 00:15 — Wel of niet verkopen zonder voorraad

Kijkt in de prijslijst of een product nog verkocht mag worden als de voorraad op is, en of het actief of verborgen moet staan. De prijs zelf wordt op dit tijdstip niet aangepast. Dezelfde controle draait ook om 07:15, 12:15 en 18:15.

### 03:00 — Catalogus kopiëren

Maakt een kopie van de ktm-shop, zodat de prijsjob later kan zien wat er veranderd is. De webshop zelf wordt niet aangepast.

### 04:00 — Producten verbergen of weer tonen

Verbergt een product als geen enkele uitvoering een bruikbare prijs heeft, als alles uitverkocht is, of als de leverancier het niet meer levert en er ook in de shop geen voorraad is. Eén uitvoering zonder prijs blijft van de productpagina af en is niet te bestellen; de andere uitvoeringen blijven te koop. Zet het product weer zichtbaar als dat niet meer zo is. Ligt er van één uitvoering nog voorraad, dan blijft het product zichtbaar.

### 04:30 — Op de webshop zetten

Zet producten op de webshop die verkoopbaar zijn en daar nog niet op staan.

### 05:00 — Douanegegevens

Vult een ontbrekende goederencode of een ontbrekend land van herkomst in. Is het land leeg, dan wordt Oostenrijk ingevuld.

### 05:30 — Barcodes

Vult lege barcodes vanuit de prijslijst. Een barcode die er al staat, blijft staan.

### 05:45 — Categorieën

Geeft een categorie aan producten die er nog geen hebben. Een categorie die al ingevuld is, blijft staan, ook als die niet de beste keuze is.

### 06:00 — Nieuwe producten (PIM)

Haalt PIM-zips van de FTP als die veranderd zijn, maakt delta’s, zet nieuwe producten in Shopify, vult ontbrekende foto’s op bestaande producten, plus YMM/complementary (ook zonder nieuwe handles). Zijn de zips hetzelfde als gisteren, dan stopt de job in seconden. Draait via de VPS. Workflow: `ktm_new_products_daily`. Deelt de KTM Shopify-schrijflock met SEO/categorie/prijs — een volle run laat die jobs wachten, Motox niet. Foto-bijwerkingen verschijnen in de ochtenddigest.

### 07:00 — Prijzen en levertijd

Werkt prijs en levertijd bij vanuit de prijslijst. De prijs is inclusief btw. Op een deel van de productsoorten komt daar 9% bovenop. Een product zonder soort krijgt die 9% niet. Dit herhaalt zich elk uur tot 23:00. Motox volgt de OEM-prijzen daarna via Synkro.

### 07:15 — Nog eens: wel of niet verkopen zonder voorraad

Dezelfde controle als om 00:15. Ook om 12:15 en 18:15.

### Maandag 07:15 — Fits-on

Werkt bij op welke motor een product past, als dat veranderd is. Maximaal 500 producten.

### 06:45 — KTM garage-filter

Bouwt de motorlijst en de compacte fitment-index opnieuw uit Supabase (`canonical_product_fits_on`) en deployt die naar Vercel (`ktm-garage`). Zo blijven nieuwe onderdelen in het motorfilter zichtbaar. Workflow: `ktm_garage_fitment_deploy.yml`. Vereist secret `VERCEL_TOKEN`.

## Motox

### 02:15 — Putoline

Haalt het Kroon-assortiment op en werkt Motox bij. De aanroep gaat via de VPS, omdat de API alleen dat adres toelaat.

- prijs, barcode en gewicht aanpassen als ze afwijken
- een lege SEO-titel of meta-omschrijving invullen, en de Shopify-categorie zetten bij de Kroon-soort
- een nieuw artikelnummer met prijs aanmaken
- alle Putoline-producten: voorraad bijhouden aan, doorverkopen (CONTINUE), publiceren op alle kanalen
- een artikel dat uit het assortiment verdwijnt alleen melden, niet verbergen
- Kroon `stock_id` (0/1/2) alleen melden — geen Shopify-aantal (Kroon levert geen qty)

De vijf oude kassa-artikelen 70630, 74016, 74026, 74036 en 74089 blijven staan. Workflow: `motox_putoline.yml`.

### 21:15 — SEO teksten

Schrijft de titel, de korte Google-tekst en de alt-tekst van de foto. Eerst producten die te koop zijn en op voorraad liggen, daarna de rest. Maximaal 4000 per nacht.

Een tekst die al goed is, blijft staan. Een tekst die alleen zegt dat het product ergens op past, of die alleen de productnaam herhaalt, wordt opnieuw geschreven. Artikelnummer en barcode worden niet aangepast.

### 03:00 — Bihr-catalogus

Haalt de nieuwste Bihr-lijst op en werkt Motox bij:

- nieuwe producten uit die lijst aanmaken, inclusief goederencode en land van herkomst als Bihr die heeft
- prijzen aanpassen als ze afwijken
- doorverkopen (inventory policy) bijwerken als Bihr wel/geen voorraad meldt (Shopify-aantal blijft meestal 0)
- een foto toevoegen als die er nog niet is
- bijwerken op welke motor een onderdeel past, als dat leeg is of niet meer klopt
- goederencode en land van herkomst bijwerken als die leeg zijn of niet meer kloppen

Producten die niet in de Bihr-lijst staan, laat deze job met rust. Prijzen van KTM, Husqvarna, GasGas en WP volgen de ktm-shop via Synkro.

### 04:30 — YMM-samenvatting

Vult `ymm_summary` en de platte velden merk, model en bouwjaar vanuit `fits_on`, als die leeg zijn of niet meer kloppen. Een product zonder `fits_on` blijft leeg. In de winter valt dit om 03:30, omdat de planner op UTC staat.

### 05:45 — Categorieën

Geeft een categorie aan producten die er nog geen hebben. Een categorie die al ingevuld is, blijft staan, ook als die niet de beste keuze is.

### 06:00 — Producten zonder foto

Verbergt producten die te koop staan maar geen foto hebben.

### 07:15 — Barcodes

Vult lege barcodes vanuit de Bihr-lijst. Een barcode die er al staat, blijft staan. Gearchiveerde producten slaat hij over.
