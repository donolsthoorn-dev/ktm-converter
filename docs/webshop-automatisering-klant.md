# Automatische webshop-updates — uitleg

Dit document beschrijft hoe het systeem de online shop (Shopify) automatisch in lijn houdt met de officiële KTM-prijs- en voorraadbestanden. Alle geplande tijden zijn **Nederlandse tijd** (Europe/Amsterdam).

Voor technische details over de planner en GitHub-workflows: [supabase-scheduler.md](./supabase-scheduler.md).

---

## Waar komt de informatie vandaan?

1. **KTM levert prijsbestanden** via een beveiligde bestandsoverdracht (FTP/SFTP).
2. Die bestanden bevatten onder andere: artikelnummer, prijs, artikelstatus, voorraadindicator en levertijd.
3. Het systeem **leest die bestanden** en vergelijkt ze met wat er op dat moment in Shopify staat.
4. Alleen **verschillen** worden bijgewerkt — niet handmatig het hele assortiment.

Daarnaast wordt ’s nachts een **kopie van de Shopify-catalogus** opgeslagen in een database. Daarmee kan het systeem betrouwbaar vergelijken en rapporten bijhouden.

---

## Overzicht: een typische nacht

| Tijd (NL) | Wat gebeurt er? |
|-----------|------------------|
| **03:00** | Catalogus bijwerken in de database (spiegel van Shopify) |
| **04:00** | Producten die niet (meer) op de webshop horen → **van de webshop** |
| **04:30** | Producten die wél op de webshop horen maar nog niet zichtbaar zijn → **zichtbaar maken** |
| **05:00** | Ontbrekende douanegegevens (HS-code / land van herkomst) aanvullen |
| **07:00–23:00** (elk heel uur) | Prijzen en levertijden bijwerken |
| **07:15, 12:15, 18:15, 00:15** | Voorraadregels en productstatus (actief / concept) bijwerken |

De zwaardere Shopify-taken lopen **niet tegelijk**: ze wachten op elkaar, zodat er geen tegenstrijdige wijzigingen ontstaan.

---

## Stap voor stap

### 1. Om 03:00 — “Wat staat er nu in de shop?”

Het systeem haalt de actuele Shopify-catalogus op en slaat die op in de database.

**Doel:** de volgende stappen weten wat er *nu* online staat, welke prijzen er staan, enzovoort.

---

### 2. Om 04:00 — “Wat moet van de webshop af?”

Producten die **niet geschikt** zijn om online te staan, worden op **concept** gezet (niet meer zichtbaar voor klanten). Dat gebeurt onder andere als:

- er **geen geldige verkoopprijs** is (prijs ontbreekt of is nul);
- **alles uitverkocht** is in Shopify (geen voorraad, klant kan niet bestellen);
- het artikel in KTM **uitgefaseerd** is (status “80”) **én** er is geen voorraad;
- KTM **“geen voorraad”** aangeeft (voorraadcode 0) **én** Shopify heeft ook echt 0 stuks.

**Belangrijk:**

- Staat er **wél voorraad in Shopify** (omdat het echt op voorraad is), dan wordt het product **niet** van de webshop gehaald — ook als KTM in het bestand “0” voorraad meldt.
- Producten die ten onrechte op concept stonden, kunnen hier weer **actief** worden — maar **niet** als ze uitverkocht zijn of geen voorraad hebben.

---

### 3. Om 04:30 — “Wat moet juist wél op de webshop?”

Producten die in Shopify **actief** zijn maar nog **niet op het Online Store-kanaal** staan, worden **zichtbaar** gemaakt — als ze voldoen aan de regels:

- artikel is **verkoopbaar** volgens KTM (niet overal status “80”);
- er is **voorraad in Shopify** of het is niet overal uitverkocht / leeg;
- product heeft een **toegestaan producttype** (geen archief, geen motorfietsen, enz.);
- er is minstens **één productafbeelding**.

Omgekeerd: producten die **wel online staan** maar dat niet meer mogen (verkeerd type, geen foto, uitverkocht, geen KTM-voorraad terwijl Shopify ook leeg is), worden **van de webshop gehaald**.

**Volgorde:** eerst opruimen (04:00), daarna publiceren (04:30). Zo worden uitverkochte artikelen niet direct daarna weer online gezet.

---

### 4. Om 05:00 — Douanegegevens

Voor artikelen waar **HS-code** of **land van herkomst** nog ontbreekt, probeert het systeem die gegevens aan te vullen.

**Doel:** completere productdata voor verzending en compliance.

---

### 5. Overdag — Prijzen en levertijden (07:00–23:00, elk heel uur)

Het systeem:

1. haalt opnieuw de **nieuwste KTM-prijsbestanden** op;
2. berekent wat er **gewijzigd** is (prijs, verwachte leverdatum);
3. werkt die wijzigingen **door naar Shopify**.

Zo blijven prijzen en levertijden actueel zonder handmatig werk.

---

### 6. Voorraadregels en status (07:15, 12:15, 18:15, 00:15)

Op vaste momenten past het systeem onder andere aan:

- **doorverkopen bij lege voorraad** (meestal: nee bij uitverkocht / geen voorraad);
- **product actief of concept:**
  - status **80** in KTM → concept, niet op webshop;
  - **geen voorraad** in KTM (code 0) → concept (mits Shopify ook geen voorraad heeft bij de nachtelijke opruimstappen);
  - anders → actief (mits verkoopbaar).

Dit sluit aan bij de nachtelijke opruim- en publish-stappen.

---

## KTM-codes in het kort

| In het prijsbestand | Betekenis in de praktijk |
|---------------------|---------------------------|
| **ArticleStatus 80** | Artikel is uitgefaseerd; hoort niet meer normaal online |
| **ArticleStatus 20** (en andere, niet 80) | Artikel hoort in principe verkoopbaar te zijn |
| **StockAvailable 0** | Geen voorraad volgens KTM |
| **StockAvailable 1** | Op voorraad |
| **StockAvailable 2** | Binnenkort weer beschikbaar |

**Vuistregel voor zichtbaarheid:** de webshop kijkt naar de **echte voorraad in Shopify**. Alleen als daar ook 0 staat, wordt een artikel als “niet beschikbaar” behandeld voor opruimen en publiceren. Heeft Shopify wél voorraad, dan blijft het artikel online — ook als KTM tijdelijk “0” meldt.

---

## Wat ziet de klant op de website?

| Situatie | Wat de klant ziet |
|----------|-------------------|
| Alles in orde | Product zichtbaar, prijs en levertijd actueel, bestellen mogelijk (mits voorraad) |
| Uitverkocht | Label “Uitverkocht” — het systeem probeert dit ’s nachts op te lossen door het product niet meer zichtbaar te maken |
| Niet meer bedoeld voor webshop | Product niet meer in de shop (concept of niet gepubliceerd op Online Store) |

---

## Handmatig vs automatisch

- **Automatisch:** de planner in Supabase start de taken op de tijden hierboven.
- **Handmatig:** dezelfde taken kunnen in GitHub worden gestart (handig voor testen of na een aanpassing).

---

## Compatibiliteit (YMM) — aparte flow

Los van de dagelijkse prijs- en voorraadflow kan **periodiek** (onder andere wekelijks) compatibiliteitsinformatie (“past op welk model”) naar Shopify worden gestuurd. Dat is een **eigen** keten; zie [supabase-ymm-pipeline.md](./supabase-ymm-pipeline.md).

---

## Samenvatting

**’s Nachts** wordt de webshop opgeschoond en aangevuld met wat volgens KTM wél online hoort. **Overdag** blijven prijzen, levertijden en voorraadregels in stapjes gelijk met KTM.

**Uitgangspunt:** wat echt in Shopify op voorraad staat, blijft online; wat uitverkocht of uitgefaseerd is en geen voorraad heeft, gaat van de webshop af.
