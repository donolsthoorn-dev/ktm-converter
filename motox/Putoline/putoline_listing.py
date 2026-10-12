"""Categorie, SEO-titel en meta-omschrijving voor Putoline.

De Kroon-soort (Viertakt motoroliën, Remvloeistoffen, …) blijft het
producttype. De Shopify-categorie wordt de bijpassende vloeistof of
verzorging, niet een onderdelengroep zoals Motor Vehicle Engine Oil Circulation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

SEO_TITLE_MAX = 70
SEO_DESC_MAX = 160

_WS = re.compile(r"\s+")
_TAG = re.compile(r"<[^>]+>")
_CONTAINER = re.compile(
    r"\b(flacon|aerosol|can|drum|vat|blik|pot|tube|trigger|pouch|stuk|bag-in-box|bib)\b\.?",
    re.I,
)
_BOILER = re.compile(r"Putoline Oil is kwaliteitsleverancier.*", re.I | re.S)
_ADVICE = re.compile(r"Raadpleeg de adviesdatabase.*", re.I | re.S)

# Shopify Standard Product Taxonomy, leaf-ids van deze winkel.
MOTOR_OIL = "gid://shopify/TaxonomyCategory/vp-1-5-5-8"
TRANSMISSION = "gid://shopify/TaxonomyCategory/vp-1-5-5-11"
FORK_FLUID = "gid://shopify/TaxonomyCategory/vp-1-5-5"
BRAKE_FLUID = "gid://shopify/TaxonomyCategory/vp-1-5-5-2"
ANTIFREEZE = "gid://shopify/TaxonomyCategory/vp-1-5-5-1"
FUEL_CLEANER = "gid://shopify/TaxonomyCategory/vp-1-5-5-5"
CLEANING = "gid://shopify/TaxonomyCategory/vp-1-5-2"
POLISH = "gid://shopify/TaxonomyCategory/vp-1-5-2-6-2"
LUBRICANT = "gid://shopify/TaxonomyCategory/ha-1-4"
GREASE = "gid://shopify/TaxonomyCategory/ha-1-4-2"
BIKE_LUBE = "gid://shopify/TaxonomyCategory/sg-4-4-1-22-4"

BUCKETS: dict[str, tuple[str, str]] = {
    "motorolie-4": (MOTOR_OIL, "Viertakt motoroliën"),
    "motorolie-2": (MOTOR_OIL, "Tweetakt motoroliën"),
    "motorolie": (MOTOR_OIL, "Motorolie"),
    "transmissie": (TRANSMISSION, "Transmissieoliën"),
    "vork": (FORK_FLUID, "Vork- en schokdemperoliën"),
    "rem": (BRAKE_FLUID, "Remvloeistoffen"),
    "koel": (ANTIFREEZE, "Koelvloeistoffen"),
    "brandstof": (FUEL_CLEANER, "Brandstof additieven"),
    "reiniging": (CLEANING, "Reiniging"),
    "filterolie": (LUBRICANT, "Luchtfilter onderhoud"),
    "ketting": (LUBRICANT, "Kettingsmeermiddelen"),
    "vet": (GREASE, "Vetten"),
    "fiets": (BIKE_LUBE, "Fiets producten"),
    "polish": (POLISH, "Bescherming"),
}

KROON_BUCKET = {
    "viertakt motoroliën": "motorolie-4",
    "tweetakt motoroliën": "motorolie-2",
    "transmissieoliën": "transmissie",
    "vork- en schokdemperoliën": "vork",
    "remvloeistoffen": "rem",
    "koelvloeistoffen": "koel",
    "brandstof additieven": "brandstof",
    "kettingsmeermiddelen": "ketting",
    "vetten": "vet",
    "reiniging": "reiniging",
}

# Deze Kroon-soorten mengen olie, reiniger en bescherming. De naam beslist.
MIXED = {"luchtfilter onderhoud", "bescherming", "kleding", "overige", "fiets producten", ""}

WEAK_TYPES = {"", "overige", "kleding", "putoline"}


def collapse(text: str) -> str:
    return _WS.sub(" ", (text or "").replace("\u00a0", " ")).strip()


def plain_text(html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html or "", flags=re.I)
    text = re.sub(r"</(p|li|div|h[1-6]|tr)>", "\n", text, flags=re.I)
    return collapse(_TAG.sub(" ", text))


def _bucket_from_name(name: str) -> str | None:
    if re.search(r"\b(dot\s*[34]|brake fluid|remvloeistof|hydraulic|clutch fluid)\b", name):
        return "rem"
    if re.search(r"\b(coolant|ice cooler|antifreeze|koelvloeistof)\b", name):
        return "koel"
    if re.search(r"\b(grease|vet)\b", name):
        return "vet"
    if re.search(r"\b(chain|ketting|chainguard)\b", name):
        return "ketting"
    if re.search(r"\b(fuel|octane|injector)\b", name):
        return "brandstof"
    if re.search(r"\b(cleaner|degreaser|sanitizer|ontvetter)\b", name):
        return "reiniging"
    if re.search(r"\b(silicone|penetrating|tyre|tire)\b", name):
        return "ketting"
    if re.search(r"\b(wax|polish|shine|proof|protect)\b", name):
        return "polish"
    if re.search(r"\b(action fluid|filter oil|filterolie)\b", name):
        return "filterolie"
    if re.search(r"\b(fork|vork|shock)\b", name):
        return "vork"
    if re.search(r"\b(gear oil|transmissie|gearbox)\b", name):
        return "transmissie"
    if re.search(r"\b(2t|2-?takt|tweetakt|mx\s*7|\bs2\b)\b", name):
        return "motorolie-2"
    if re.search(r"\b(\d+w-\d+|4t|viertakt|\bs4\b)\b", name):
        return "motorolie-4"
    return None


def bucket_for(product_type: str, title: str) -> str:
    kind = collapse(product_type).lower()
    name = collapse(title).lower()
    if kind not in MIXED and kind in KROON_BUCKET:
        return KROON_BUCKET[kind]
    guessed = _bucket_from_name(name)
    if guessed:
        if kind == "fiets producten" and guessed == "ketting":
            return "fiets"
        if kind == "fiets producten" and guessed == "polish":
            return "polish"
        return guessed
    if kind in KROON_BUCKET:
        return KROON_BUCKET[kind]
    if kind == "luchtfilter onderhoud":
        return "reiniging"
    if kind == "fiets producten":
        return "fiets"
    return "ketting"


_LEADING_SIZE = re.compile(r"^(\d+(?:[.,]\d+)?\s*(?:ml|l|g|kg))\b", re.I)


def seo_title(title: str) -> str:
    name = collapse(_CONTAINER.sub(" ", title))
    name = collapse(re.sub(r"\bputoline\b", " ", name, flags=re.I))
    size = _LEADING_SIZE.match(name)
    if size:
        name = collapse(f"{name[size.end():]} {size.group(1)}")
    base = f"Putoline {name}" if name else "Putoline"
    words = base.split()
    while words and len(" ".join(words)) > SEO_TITLE_MAX:
        words.pop()
    return " ".join(words)


_STOP = re.compile(
    r"\b(Aanwijzingen voor gebruik|Toepassing|Verpakking|Veiligheidsblad|Productinformatieblad|Dichtheid bij)\b",
    re.I,
)
_TRAIL = re.compile(r"[:,]?\s*met de volgende eigenschappen\s*:?\s*$", re.I)


def _sentence(text: str) -> str:
    text = collapse(text).rstrip(" ,;:-")
    if not text:
        return ""
    if text[-1] not in ".!?":
        text += "."
    return text


def seo_description(title: str, body_html: str) -> str:
    text = plain_text(body_html)
    text = _BOILER.sub(" ", text)
    text = _ADVICE.sub(" ", text)
    text = _STOP.split(text)[0]
    chunks = [collapse(chunk) for chunk in re.split(r"\s+-\s+", text) if collapse(chunk)]
    if chunks:
        chunks[0] = _TRAIL.sub("", chunks[0]).strip()
    parts = []
    for chunk in chunks:
        parts.extend(collapse(part) for part in re.split(r"(?<=[.!?])\s+", chunk) if collapse(part))
    chosen: list[str] = []
    total = 0
    for part in parts:
        sentence = _sentence(part)
        if not sentence or len(sentence) < 12:
            continue
        if len(sentence) > SEO_DESC_MAX:
            if chosen:
                break
            lowered = sentence.lower()
            for marker in (" op basis", " met een", " die ", " welke ", " waardoor ", " zodat "):
                idx = lowered.find(marker)
                if 50 < idx <= SEO_DESC_MAX:
                    return _sentence(sentence[:idx])
            comma = sentence.rfind(",", 40, SEO_DESC_MAX)
            if comma > 40:
                return _sentence(sentence[:comma])
            cut = sentence[: SEO_DESC_MAX - 1].rsplit(" ", 1)[0].rstrip(" ,;:-")
            return _sentence(cut) or seo_title(title)
        extra = len(sentence) if not chosen else len(sentence) + 1
        if total + extra > SEO_DESC_MAX:
            break
        chosen.append(sentence)
        total += extra
        if len(chosen) == 2:
            break
    if chosen:
        return " ".join(chosen)
    fallback = seo_title(title)
    return fallback if fallback.endswith(".") else f"{fallback}."


@dataclass(frozen=True)
class Listing:
    category_id: str
    product_type: str
    seo_title: str
    seo_description: str


def listing_for(*, title: str, product_type: str, body_html: str) -> Listing:
    bucket = bucket_for(product_type, title)
    category_id, dutch = BUCKETS[bucket]
    current = collapse(product_type)
    if current.lower() in WEAK_TYPES:
        product_type_out = dutch
    else:
        product_type_out = current
    return Listing(
        category_id=category_id,
        product_type=product_type_out,
        seo_title=seo_title(title),
        seo_description=seo_description(title, body_html),
    )
