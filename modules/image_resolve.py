"""
Zoekt lokale bestanden bij image-paden uit de XML (bijv. PHO/NMON/foo.jpg).

De oude logica gebruikte alleen de bestandsnaam én één pad per naam (laatste wint).
Daardoor ontbreken er matches als namen dubbel voorkomen of als alleen het volledige
relatieve pad klopt.

KTM/WP/HSQ-bestanden op schijf hebben vaak een Celum-ID in de naam
(`pho_nmon_430720_a4600…jpg`) terwijl de XML die ID weglaat
(`pho_nmon_a4600…jpg`). Daarom: na exacte match ook zoeken op artikelnummer-token.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

# Artikelachtige tokens in mediabestandsnamen (SKU/handle-fragment).
_ARTICLE_TOKEN_RE = re.compile(r"(?i)(?<![a-z0-9])([a-z]*\d[a-z0-9]{7,})(?![a-z0-9])")


def build_basename_index(input_root: Path) -> tuple[dict[str, list[Path]], dict[str, list[Path]]]:
    """Per exacte basename en per lowercase basename: alle gevonden paden onder input_root."""
    by_exact: dict[str, list[Path]] = defaultdict(list)
    by_lower: dict[str, list[Path]] = defaultdict(list)
    root = input_root.resolve()
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        by_exact[p.name].append(p)
        by_lower[p.name.lower()].append(p)
    return dict(by_exact), dict(by_lower)


def _article_tokens_from_basename(name: str) -> list[str]:
    """Haal artikelachtige stukken uit een XML-bestandsnaam."""
    stem = Path(name).stem.lower()
    # Negeer standaard prefix/suffix-ruis voor tokenextractie.
    stem = re.sub(r"^pho_[a-z]+_", "", stem)
    stem = re.sub(r"__sall__.*$", "", stem)
    tokens = _ARTICLE_TOKEN_RE.findall(stem)
    # Uniek, langste eerst (specifieker).
    uniq: list[str] = []
    seen: set[str] = set()
    for t in sorted(tokens, key=len, reverse=True):
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def _fuzzy_match_by_article_token(
    name: str,
    by_lower: dict[str, list[Path]],
) -> Path | None:
    """
    Match lokale bestanden die het artikelnummer uit de XML-naam bevatten,
    ook als er een Celum-ID of kleine naamvariatie tussen zit.
    """
    tokens = _article_tokens_from_basename(name)
    if not tokens:
        return None

    # Beschrijvende delen uit XML-naam (zonder artikel) voor ranking.
    stem = Path(name).stem.lower()
    stem = re.sub(r"^pho_[a-z]+_", "", stem)
    stem = re.sub(r"__sall__.*$", "", stem)
    desc_parts = [
        p
        for p in re.split(r"[_\W]+", stem)
        if len(p) >= 4 and p not in tokens and not p.isdigit()
    ]

    candidates: list[Path] = []
    for token in tokens:
        for bname, paths in by_lower.items():
            if token in bname and "fallback_no_picture" not in bname:
                candidates.extend(paths)
        if candidates:
            break

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    def score(p: Path) -> tuple[int, int]:
        n = p.name.lower()
        hit = sum(1 for d in desc_parts if d in n)
        # Prefer shorter names when score ties (minder varianten).
        return (-hit, len(n))

    return sorted(candidates, key=score)[0]


def resolve_local_image(
    ref: str,
    input_root: Path,
    by_exact: dict[str, list[Path]],
    by_lower: dict[str, list[Path]],
) -> Path | None:
    """
    Vind een lokaal pad voor een XML-referentie.

    1) input_root / relatief_pad (met traversaal-check)
    2) Unieke basename (exact)
    3) Basename case-insensitive; bij meerdere: voorkeur voor pad dat op ref eindigt
    4) Fuzzy: artikelnummer-token uit XML-naam komt voor in een lokale bestandsnaam
       (dekt Celum-ID-prefix in lokale PHO-bestanden)
    """
    raw = (ref or "").strip().replace("\\", "/")
    if not raw or raw.endswith("/"):
        return None

    root = input_root.resolve()
    candidate = (root / raw).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    if candidate.is_file():
        return candidate

    name = Path(raw).name
    if not name:
        return None

    lst = by_exact.get(name)
    if lst is not None and len(lst) == 1:
        return lst[0]

    low_list = by_lower.get(name.lower())
    if low_list:
        if len(low_list) == 1:
            return low_list[0]
        raw_low = raw.lower()
        for c in low_list:
            if c.as_posix().lower().endswith(raw_low):
                return c
        return low_list[0]

    return _fuzzy_match_by_article_token(name, by_lower)
