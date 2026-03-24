"""
Zoekt lokale bestanden bij image-paden uit de XML (bijv. PHO/NMON/foo.jpg).

De oude logica gebruikte alleen de bestandsnaam én één pad per naam (laatste wint).
Daardoor ontbreken er matches als namen dubbel voorkomen of als alleen het volledige
relatieve pad klopt.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path


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
    if not low_list:
        return None
    if len(low_list) == 1:
        return low_list[0]

    raw_low = raw.lower()
    for c in low_list:
        if c.as_posix().lower().endswith(raw_low):
            return c
    return low_list[0]
