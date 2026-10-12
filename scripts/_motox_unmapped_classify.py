#!/usr/bin/env python3
"""Classificeer ongemapte Motox-types naar voorgestelde collecties (geen Shopify-writes)."""

from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "output" / "motox_unmapped_types.json"
OUT = ROOT / "output" / "motox_unmapped_types_proposals.json"

OEM_VENDORS = {
    "ktm",
    "husqvarna",
    "husqvarna motorcycles",
    "wp",
    "wp suspension",
    "gasgas",
    "gas gas",
    "ktm-shop.nl",
}

TIRE_ROOTS = {
    "trail",
    "custom",
    "sport-touring",
    "hypersport",
    "cross / mx / sx",
    "utilitair",
    "enduro / rallye-raid",
    "slicks",
    "competitiebanden met profiel",
    "binnenbanden",
    "mousse",
    "atv 3w street",
    "atv sport / racing",
    "scooter - radiaal",
    "regenbanden",
    "winterbanden scooter",
    "bromfiets",
    "trial",
    "flat track",
    "off-road amateur",
    "competitie",
    "supermotard",
    "tires & accessories",
}

TIRE_HINTS = re.compile(
    r"band|tyre|tire|mousse|slick|strand races|ppi selection|pirelli-selectie|"
    r"pakketdeals banden|opruiming banden|banden selectie",
    re.I,
)


def fold(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", s or "")
        .encode("ascii", "ignore")
        .decode()
        .casefold()
        .strip()
    )


def first_seg(t: str) -> str:
    return t.split(" > ")[0].strip()


def is_tire(t: str, samples: list) -> bool:
    f = fold(first_seg(t))
    if f in TIRE_ROOTS or f.startswith("tires"):
        return True
    if TIRE_HINTS.search(t):
        return True
    titles = " ".join((s.get("title") or "") for s in samples)
    if re.search(
        r"\bBand\b|Bandenset|Tire Pack|Pneumatico|Binnenband|Mousse", titles
    ):
        return True
    return False


def is_bike(t: str) -> bool:
    tl = t.lower()
    prefixes = (
        "bicycle",
        "fiets",
        "e mtb",
        "e tr",
        "e city",
        "sl e",
        "trekking",
        "headset",
        "stem-",
        "stems",
        "grips-",
        "pedals",
        "saddles",
        "waterbottle",
        "watterbottle",
        "tools-sets",
        "pumps-",
        "lights-",
        "oil-chain",
        "jacket-jacket",
        "shoes-mtb",
        "spare parts-",
        "chainstray",
        "computer-",
        "bags-",
        "29 inch",
    )
    if any(tl.startswith(p) or p in tl for p in prefixes):
        if tl.startswith("bags and"):
            return False
        return True
    return t in {
        "Shimano Groups",
        "Shift & Brake Parts",
        "Bike Tools",
        "Electric Balance Bikes",
        "MTB",
        "MTB Hardtail",
        "Kids",
    }


def is_ktm_oem(t: str, x: dict) -> bool:
    if t == "Partstream":
        return True
    if is_bike(t) or t in {
        "Fietsen",
        "Fietsbanden",
        "Fiets binnenbanden",
        "Electric Balance Bikes",
        "29 inch",
        "Opruiming Fiets > Fietsbanden",
        "Opruiming Fiets > Fiets binnenbanden",
    }:
        return False
    samples = x.get("samples") or []
    vends = [(s.get("vendor") or "").strip() for s in samples]
    if not vends:
        return False
    oem_n = sum(1 for v in vends if v.casefold() in OEM_VENDORS)
    return oem_n >= max(1, (len(vends) + 1) // 2)


def tire_extra(t: str) -> str:
    fl = fold(first_seg(t))
    path = fold(t)
    if "binnenband" in fl or "mousse" in fl:
        return "Sub: binnenbanden / mousse."
    if "fiets" in fl:
        return ""
    if "cross" in fl or "mx" in fl or "strand" in path or "off-road amateur" in path:
        return "Sub: MX / offroad."
    if "trail" in fl or "enduro" in fl or "rallye" in fl or "trial" in fl:
        return "Sub: adventure / enduro."
    if "hypersport" in fl or "slick" in fl or "competitie" in fl or "regenband" in fl:
        return "Sub: race / hypersport."
    if "sport-touring" in fl or "custom" in fl:
        return "Sub: straat / touring / custom."
    if "atv" in fl or "utilitair" in fl:
        return "Sub: ATV / utilitair."
    if "scooter" in fl or "bromfiets" in fl or "winterbanden" in fl:
        return "Sub: scooter / bromfiets."
    return "Sub: overig / opruiming."


def propose(t: str, x: dict) -> list[tuple[str, str, str]]:
    """Return list of (collection, why, confidence)."""
    samples = x.get("samples") or []
    titles = " ".join((s.get("title") or "") for s in samples)
    fl = fold(first_seg(t))

    if is_tire(t, samples) and "fiets" not in fold(t):
        extra = tire_extra(t)
        why = (
            "e-bihr schrijft de Bihr-categorieboom in Type. Er is geen Banden-collectie; "
            "smart collections matchen TYPE alleen op exacte EQUALS. "
            + extra
        )
        return [("Banden (nieuw)", why, "hoog")]

    if is_ktm_oem(t, x):
        return [
            (
                "niet Motox (KTM/HSQ/WP)",
                "KTM-vendor / Partstream / PowerParts — alleen ktmshop. "
                "Landt nu hooguit in collectie KTM producten via vendor-regel.",
                "hoog",
            )
        ]

    if is_bike(t) or "fiets" in fold(t):
        wear = (
            "jersey",
            "glove",
            "jacket",
            "pant",
            "short",
            "helmet",
            "shoe",
            "sock",
            "innerpant",
            "workwear",
            "heads and scarfs",
        )
        if any(k in t.lower() for k in wear):
            return [
                (
                    "Fietskleding",
                    "Fiets kleding/helm/schoenen. Collectie Fietskleding dekt al soortgelijke types.",
                    "hoog",
                )
            ]
        bikes = {
            "Fietsen",
            "E MTB Fully",
            "E MTB HT",
            "E TrOnroad",
            "E City",
            "MTB Hardtail",
            "Electric Balance Bikes",
            "SL E TrOnroad",
            "SL E Gravel",
            "SL E Kids",
            "Trekking Onroad",
            "Kids",
            "Bicycle",
        }
        if t in bikes:
            return [
                (
                    "MTB Fietsen",
                    "Complete fietsen. Bestaande collecties MTB Fietsen / Mountainbikes KTM of SCOTT / STACYC.",
                    "hoog",
                )
            ]
        return [
            (
                "Fietsonderdelen",
                "Fiets-onderdeel of accessoire. Bestaande collectie Fietsonderdelen / Fiets.",
                "hoog",
            )
        ]

    table: list[tuple[tuple[str, ...], str, str, str]] = [
        (("Broeken",), "Broeken", "Collectie Broeken matcht MX Broek / textiel / regen, niet type Broeken.", "hoog"),
        (("Handschoenen", "100% Gloves", "Gloves"), "Handschoenen", "Collectie Handschoenen matcht alleen Off-road handschoenen.", "hoog"),
        (
            (
                "Helmen",
                "Helmets",
                "BELL Street",
                "BELL Off-Road",
                "Custom 500/500 DLX",
                "Bullitt DLX",
                "Bullitt Carbon",
                "Eliminator",
                "Moto-9 Mips",
                "Airoh",
            ),
            "Helmen",
            "Collectie Helmen heeft al BELL-types; deze variant ontbreekt als EQUALS.",
            "hoog",
        ),
        (("Jassen", "Jackets"), "Jassen", "Collectie Jassen matcht Enduro/textiel/regenjassen, niet dit type.", "midden"),
        (("Shirts", "T-shirt & polo", "T-Shirts", "Jerseys", "T-shirts and Polos"), "Shirts", "Collectie Shirts / Kleding.", "hoog"),
        (
            ("Goggle", "Goggle Acc", "Goggles", "Goggle Case", "Lensen", "Rip n Roll", "GOGGLES 2019"),
            "Brillen",
            "Collectie Brillen matcht Crossbril(len), niet Goggle/Lensen.",
            "hoog",
        ),
        (("Laarzen/Schoenen", "Boots", "Slippers & schoenen"), "Laarzen", "Collectie Laarzen matcht Crosslaarzen.", "midden"),
        (
            ("Protectiedelen", "Asterisk", "POD", "Atlas Brace", "Inuteq Cooling Vest"),
            "Body protectie",
            "Lichaamsprotectie; collecties Body protectie / Protectie.",
            "hoog",
        ),
        (
            ("Caps and beanies", "Headwear", "Caps & petten", "Capuchon truien", "Casual kleding", "Lifestyle", "Boardshorts", "Functional underwear"),
            "Sportkleding en casual",
            "Casual/lifestyle, geen rij-outfit.",
            "midden",
        ),
        (("Kettingkit", "D.I.D Off-Road Ketting"), "Ketting", "Collectie Ketting matcht alleen type Ketting, niet kits.", "hoog"),
        (("Graphics & Stickers", "Decalkit", "Decals", "Factory Effex"), "Stickers", "Collectie Stickers heeft al stickersets, niet Graphics/Decalkit.", "hoog"),
        (("Off-Road Zadelhoes", "Seat Cover"), "Zadels", "Collectie Zadels matcht Zadelhoes, niet Off-Road Zadelhoes.", "hoog"),
        (("Cover luchtfilterbehuizing",), "Plastics", "Luchtfilterdeksels zijn bodywork-plastics.", "hoog"),
        (
            (
                "Radiateurlamellen / Radiateurhoes",
                "Frame Protectors",
                "Zelfklevende antislip",
                "Skid Plates",
                "Chain Guides/Sliders",
                "Aandrijfriem beschermer",
                "Plastics",
            ),
            "Plastics",
            "Protectie/plastics rijwiel; collectie Plastics (of Bescherming voertuig).",
            "hoog",
        ),
        (("Bescherming voorvork", "Bescherming carter", "Bobbins achterbrug"), "Bescherming voertuig", "Bestaande collectie Bescherming voertuig.", "hoog"),
        (("Wheels Haan Rear", "Wheels Haan Front", "Opruiming Rijwielgedeelte", "Velglint"), "Wielen", "Complete wielen / velglint; collectie Wielen.", "hoog"),
        (("Bagage rijder", "Zachte bagage", "Hydratatie", "Hydration system", "Backpack"), "Bagage", "Collectie Bagage matcht nu vooral rekken/toebehoren.", "midden"),
        (
            ("Spiegel (Universele)", "Spiegel (OE Type)", "Spiegel - Universele", "Spiegel OE Type", "Accessoires spiegel"),
            "Spiegels",
            "Collectie Spiegels matcht Originele/Universele spiegel, niet deze spellingen.",
            "hoog",
        ),
        (
            (
                "Richtingaanwijzer (OE type)",
                "Achterlicht origineel",
                "Achterlicht universeel",
                "Extra verlichting",
                "Headlight",
                "Indicator adapter cable",
                "Relais en weerstand",
                "Claxon",
            ),
            "Elektriciteit",
            "Verlichting / elektrische componenten.",
            "hoog",
        ),
        (("Startkit",), "Rijwieldelen", "Holeshot devices; geen eigen collectie. Rijwieldelen is het breedste haakje.", "midden"),
        (
            ("Verhoging + verlagingssets", "Hoogteverstelling zadel", "Veervoorspanningsregelaar", "Dichting schokdemper en stofkap", "Stofkap", "Voorvork en Onderdelen"),
            "Ophanging",
            "Vork/schok/verlaging; collectie Ophanging (stofkappen ook Keringen).",
            "hoog",
        ),
        (("Kettingspanner",), "Ketting", "Kettingspanners horen bij aandrijving.", "hoog"),
        (
            ("Cilinderbus", "Drijfstangpen", "Zuigerpen", "Zuiger", "Zuigerpen circlips", "Motorblok"),
            "Bovenblok",
            "Motorinterne delen; Bovenblok of Rotatief geheel.",
            "midden",
        ),
        (("Reparatiekit koppelingsslaafcilinder",), "Koppeling", "Collectie Koppeling.", "hoog"),
        (("Koelmiddel", "Radiateurdoppen"), "Koeling", "Koelvloeistof / radiateur; collectie Koeling.", "midden"),
        (("Air filter Powerflow kit", "Twin Air"), "Filtratie", "Collectie Filtratie heeft al Powerflow kits / luchtfilters.", "hoog"),
        (
            ("XTrig Triple clamps", "XTrig Bar mounts", "Bar mounts", "Handlebar", "Stuur", "Fatbar", "Pad", "Renthal Hard Anodized Limited Edition"),
            "Sturen",
            "Stuur / kroonplaat / barpad; Sturen of Kroonplaten / Stuur en bevestiging.",
            "hoog",
        ),
        (("Schakel en rempedalen", "Sunline"), "Voetsteunen", "Collectie Voetsteunen bevat al schakelpook / rempedaal.", "hoog"),
        (("Motorhoezen", "Transport", "Transport - Garage - Paddock", "DRC Hardware"), "Transport - Garage - Paddock", "Bestaande collectie matcht specifiekere types, niet dit overkoepelende type.", "midden"),
        (("Antidiefstal",), "Transport - Garage - Paddock", "Geen sloten-collectie. Dichtstbijzijnde bestaande bak, of nieuw: Antidiefstal.", "midden"),
        (("Intercom",), "Helmen", "Helmaccessoire; Helmen is het logische haakje.", "midden"),
        (("Benzinekraan", "Benzineslangen"), "Inlaat", "Brandstofsysteem; collectie Inlaat.", "hoog"),
        (("Uitlaat dempervullingen", "Opruiming Motor & Uitlaat"), "Uitlaat en dempers", "Collectie Uitlaat en dempers.", "hoog"),
        (("Nummerplaat met verlichting en reserveonderdelen",), "Kentekenplaathouder", "Nummerplaat/verlichting.", "midden"),
        (("Reserveonderdelen Motor",), "Koppeling", "Samples zijn Hinson/Boyesen koppeling; type is te breed — liever splitsen.", "laag"),
        (("Reserveonderdelen Rijwielgedeelte",), "Rijwieldelen", "Generiek resttype.", "laag"),
        (("Reserveonderdelen Gereedschap",), "Gereedschap", "Bestaande gereedschap-collectie.", "midden"),
        (
            ("Putoline Care Products", "Putoline Off Road", "Putoline Quad", "Putoline Road", "Putoline Scooter", "Oil Products", "Hygiëne & reininger"),
            "Onderhouds producten",
            "Vloeistoffen/verzorging.",
            "hoog",
        ),
        (("Merkspecifieke gereedschappen > Nokkenas",), "Merkspecifieke gereedschappen", "Parent-type zit al in die collectie.", "hoog"),
        (("Textiel en accessoires > Opruiming Slijtageonderdelen > Vacuumslangen",), "Radiatorslangen", "Collectie Radiatorslangen matcht Vacuumslangen.", "hoog"),
        (("Jet Ski romp aftappluggen > Olie aftappluggen",), "Onderblok", "Collectie Onderblok heeft Olie aftappluggen.", "hoog"),
        (("Onderhoud van leer & textiel > Helmreiniger",), "Helmen", "Helmreiniger staat al als type in Helmen.", "hoog"),
        (("Accessoires", "ACCESSOIRES"), "Brillen", "Samples zijn tear-offs/lenzen; collectie Brillen.", "midden"),
        (("Alpinestars",), "Laarzen", "Samples zijn laarzenclips; type is alleen de merknaam.", "midden"),
        (("Handschoenen",), "Handschoenen", "", "hoog"),
        (("Helmen",), "Helmen", "", "hoog"),
        (("Broeken",), "Broeken", "", "hoog"),
        (("Shirts",), "Shirts", "", "hoog"),
    ]
    for keys, coll, why, conf in table:
        if t in keys:
            return [(coll, why, conf)]

    brand_years = {
        "Alpinestars 2018",
        "Fox Racing 2018",
        "Fox Racing 2017",
        "Fox Racing",
        "Thor",
        "THOR",
        "THOR 2018",
        "SCOTT 2018",
        "SCOTT",
        "Oneal 2016",
        "Kenny 2018",
        "Kenny Racing",
        "Kenny Racing 2018",
        "Troy Lee Designs 2016",
        "Troy Lee Design 2016",
        "SHIFT 2018",
        "FLY Racing",
        "Leatt 2016",
        "UFO 2018",
        "UFO 2016",
        "Acerbis 2017",
        "Acerbis",
        "Firstracing 2017",
        "Moose Racing 2018",
        "Jopa Racing 2017",
        "One industries",
        "Sinisalo",
    }
    if t in brand_years:
        return [
            (
                "Kleding",
                "Merk/jaartal als Type i.p.v. productgroep. Mix van kleding/gear — Type opschonen of breed aan Kleding, daarna splitsen.",
                "laag",
            )
        ]

    merknamen = {
        "Zeta",
        "Pro Taper",
        "Odi",
        "Domino",
        "Pro Grip",
        "X-GRIP",
        "ART",
        "Tecnium",
        "TMV",
        "Race Tech",
        "Overige merken",
    }
    if t in merknamen:
        return [
            (
                "eerst Type opschonen",
                "Merk als Type. Samples zijn mix (handvatten, hendels, protectie). Eén collectie zou het menu vervuilen.",
                "laag",
            )
        ]

    if t in {
        "Big Sale Off-Road & ATV",
        "Sale",
        "Opruiming Voertuiguitrusting",
        "Opruiming Slijtageonderdelen",
        "Display",
    }:
        return [
            (
                "geen vaste productcollectie",
                "Sale/opruiming/display. Beter een sale-collectie op tag of prijs, niet op dit Type.",
                "midden",
            )
        ]

    if t in {
        "¯\\_(ツ)_/¯",
        "Divers",
        "Diverse",
        "Overige",
        "Overige Toebehoren",
        "Other",
        "Accessiores",
        "Other Accessoires",
        "Electra",
        "Honda",
        "Kawasaki",
        "Husqvarna",
    }:
        return [
            (
                "eerst Type opschonen",
                "Dump-type of merknaam. Samples heterogeen; niet 1-op-1 koppelen.",
                "laag",
            )
        ]

    if t.startswith("TOOLING"):
        return [("Gereedschap", "Engelse Bihr-categorieboom als Type.", "midden")]
    if t.startswith("VEHICLE PARTS"):
        return [("Ophanging", "Samples zijn KYB Factory Kit vork; collectie Ophanging.", "midden")]

    if "helm" in fl:
        return [("Helmen", "Type bevat helm.", "midden")]
    if "glove" in fl or "handschoen" in fl:
        return [("Handschoenen", "Type wijst op handschoenen.", "midden")]
    if "boot" in fl or "laars" in fl:
        return [("Laarzen", "Type wijst op laarzen.", "midden")]
    if "jacket" in fl or "jas" in fl:
        return [("Jassen", "Type wijst op jassen.", "midden")]
    if "goggle" in fl or "bril" in fl:
        return [("Brillen", "Type wijst op brillen.", "midden")]
    if "protect" in fl:
        return [("Body protectie", "Type wijst op protectie.", "midden")]
    if "tool" in fl or "gereedschap" in fl:
        return [("Gereedschap", "Type wijst op gereedschap.", "midden")]

    _ = titles, fl
    return [
        (
            "handmatig beoordelen",
            "Geen eenduidige match op type/samples.",
            "laag",
        )
    ]


def sample_titles(x: dict, n: int = 2) -> str:
    out = []
    for s in x.get("samples") or []:
        t = (s.get("title") or "").strip()
        if t:
            out.append(t[:80])
        if len(out) >= n:
            break
    return " · ".join(out)


def vendors(x: dict) -> str:
    vs = sorted({(s.get("vendor") or "").strip() for s in x.get("samples") or [] if s.get("vendor")})
    return ", ".join(vs[:4]) + ("…" if len(vs) > 4 else "")


def main() -> None:
    d = json.loads(SRC.read_text())
    details = {x["type"]: x for x in d["details"]}
    unmatched = d["unmatched_positive"]

    rows = []
    buckets: dict[str, list] = defaultdict(list)
    for t in unmatched:
        x = details.get(t) or {"count": 0, "samples": [], "type": t}
        props = propose(t, x)
        coll, why, conf = props[0]
        row = {
            "type": t,
            "count": x.get("count") or 0,
            "proposal": coll,
            "why": why,
            "confidence": conf,
            "vendors": vendors(x),
            "samples": sample_titles(x),
            "is_path": " > " in t,
            "in_ktm_vendor_collection": "KTM producten"
            in (x.get("sample_product_collections") or []),
        }
        rows.append(row)
        buckets[coll].append(row)

    rows.sort(key=lambda r: (-r["count"], r["type"]))
    bucket_sum = []
    for coll, items in buckets.items():
        bucket_sum.append(
            {
                "proposal": coll,
                "types": len(items),
                "products": sum(i["count"] for i in items),
            }
        )
    bucket_sum.sort(key=lambda b: -b["products"])

    motox_rows = [r for r in rows if r["proposal"] != "niet Motox (KTM/HSQ/WP)"]
    oem_rows = [r for r in rows if r["proposal"] == "niet Motox (KTM/HSQ/WP)"]
    band_rows = [r for r in rows if r["proposal"].startswith("Banden")]
    path_rows = [r for r in motox_rows if r["is_path"]]

    out = {
        "shop": d["shop"],
        "fetched": "live Shopify Motox · productTypes + smart collection TYPE EQUALS",
        "totals": {
            "collections": d["collection_count"],
            "all_types": d["all_type_count"],
            "oem_prefix_types_excluded": d["oem_type_count"],
            "motox_types": d["motox_type_count"],
            "unmapped_types": len(rows),
            "unmapped_products": sum(r["count"] for r in rows),
            "unmapped_zero_products": sum(1 for r in rows if r["count"] == 0),
            "ktm_oem_types": len(oem_rows),
            "ktm_oem_products": sum(r["count"] for r in oem_rows),
            "motox_relevant_types": len(motox_rows),
            "motox_relevant_products": sum(r["count"] for r in motox_rows),
            "banden_types": len(band_rows),
            "banden_products": sum(r["count"] for r in band_rows),
            "ebihr_path_types": len(path_rows),
            "mapped_motox_types": d["motox_type_count"] - len(rows),
        },
        "buckets": bucket_sum,
        "rows": rows,
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Geschreven {OUT}")
    print(json.dumps(out["totals"], indent=2))
    print("top buckets:")
    for b in bucket_sum[:15]:
        print(f"  {b['products']:5d} prod  {b['types']:3d} types  {b['proposal']}")


if __name__ == "__main__":
    main()
