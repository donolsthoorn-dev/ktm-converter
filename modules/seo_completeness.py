"""Voorstellen voor SEO-titel, alt-tekst en een template-fallback voor de meta.

De meta-omschrijving voor de dry-run komt uit het model (zelfde pad als Motox).
Passendheid ("Past op") en HOMNN worden niet in de tekst gezet of uit de
productcopy gehaald. Bedoeld voor fill-if-empty.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass

SEO_TITLE_MAX = 70
SEO_DESC_MAX = 160
ALT_MAX = 125

_WS = re.compile(r"\s+")
_STYLE_OR_SCRIPT = re.compile(r"(?is)<(script|style)[^>]*>.*?</\1>")
_CSS_RULE = re.compile(r"\.[A-Za-z0-9_-]+\{[^}]*\}")
_HOMNN = re.compile(r"(?i)\bHOMNN(?:[_\s-]?EU)?\b")
_APPROVALS_HOMNN = re.compile(r"(?i)\bApprovals:\s*HOMNN(?:[_\s-]?EU)?\b")
_SKIP_PROP_LABELS = (
    "approvals",
    "colour",
    "color",
    "surface",
    "material",
    "model year",
    "type",
)


def collapse_ws(text: str) -> str:
    return _WS.sub(" ", (text or "").replace("\u00a0", " ")).strip()


def strip_html(raw: str) -> str:
    t = _STYLE_OR_SCRIPT.sub(" ", raw or "")
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.I)
    t = re.sub(r"</(p|li|div|h[1-6]|tr)>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = _CSS_RULE.sub(" ", html.unescape(t))
    return collapse_ws(t)


def truncate(text: str, max_len: int) -> str:
    s = collapse_ws(text)
    if len(s) <= max_len:
        return s
    cut = s[: max_len - 1].rsplit(" ", 1)[0]
    if len(cut) < max_len // 2:
        cut = s[: max_len - 1]
    return cut.rstrip(" ,;.-") + "…"


def body_has_homnn(html_or_text: str) -> bool:
    return bool(_HOMNN.search(html_or_text or ""))


def strip_homnn_plain(text: str) -> str:
    t = _APPROVALS_HOMNN.sub(" ", text or "")
    t = _HOMNN.sub(" ", t)
    return collapse_ws(t)


def _split_concatenated_bullets(plain: str) -> str:
    """KTM-XML plakt bullets vaak zonder newlines: '…starting Made of…'."""
    t = re.sub(r"(?<=[a-z0-9]) (?=[A-Z])", "\n", plain or "")
    t = re.sub(r"(?i)\s+(Approvals:)", r"\n\1", t)
    return t


def marketing_lines(plain: str) -> list[str]:
    """Feature-zinnen zonder interne XML-eigenschappen."""
    lines: list[str] = []
    split_src = _split_concatenated_bullets(plain or "")
    for chunk in re.split(r"[\n•]+", split_src):
        line = strip_homnn_plain(chunk)
        if not line:
            continue
        low = line.lower()
        if any(low.startswith(lab) or low.startswith(lab + ":") for lab in _SKIP_PROP_LABELS):
            continue
        if ":" in line and line.split(":", 1)[0].strip().lower() in _SKIP_PROP_LABELS:
            continue
        if "approvals:" in low:
            continue
        if len(line) < 8:
            continue
        if line not in lines:
            lines.append(line)
    return lines


def propose_seo_title(title: str, vendor: str = "KTM") -> str:
    t = collapse_ws(title)
    v = collapse_ws(vendor) or "KTM"
    if not t:
        return truncate(f"Origineel {v}-onderdeel", SEO_TITLE_MAX)
    if t.lower().startswith(v.lower()):
        base = t
    else:
        base = f"{v} {t}"
    return truncate(base, SEO_TITLE_MAX)


def propose_seo_description(
    *,
    title: str,
    vendor: str,
    body_html: str,
    ymm_summary: str = "",
) -> str:
    v = collapse_ws(vendor) or "KTM"
    t = collapse_ws(title)
    lines = marketing_lines(strip_html(body_html))
    _ = ymm_summary  # passendheid hoort niet in de SEO-tekst
    bits = [f"Origineel {v}-onderdeel"]
    if lines:
        bits.append(lines[0])
        if len(lines) > 1 and len(lines[1]) < 70:
            bits.append(lines[1])
    elif t and len(t) <= 80:
        bits.append(t)
    text = ". ".join(b.rstrip(".") for b in bits if b)
    text = collapse_ws(text.replace("..", "."))
    if not text.endswith("."):
        text += "."
    return truncate(text, SEO_DESC_MAX)


def propose_image_alt(title: str, vendor: str = "KTM") -> str:
    return propose_seo_title(title, vendor)[:ALT_MAX]


def propose_body_html_strip_homnn(body_html: str) -> str | None:
    """Geef opgeschoonde HTML terug als HOMNN erin zat, anders None (geen wijziging)."""
    raw = body_html or ""
    if not body_has_homnn(raw):
        return None

    def _drop_homnn_chunks(m: re.Match[str]) -> str:
        inner = m.group(1)
        if _HOMNN.search(inner) and len(strip_homnn_plain(strip_html(inner))) < 8:
            return ""
        cleaned = _APPROVALS_HOMNN.sub("", inner)
        cleaned = _HOMNN.sub("", cleaned)
        return m.group(0).replace(inner, cleaned)

    out = re.sub(r"<(li|p|td|span)([^>]*)>(.*?)</\1>", _drop_homnn_chunks, raw, flags=re.I | re.S)
    out = _APPROVALS_HOMNN.sub("", out)
    out = _HOMNN.sub("", out)
    out = re.sub(r"(?i)<(li|p)[^>]*>\s*</\1>", "", out)
    if collapse_ws(out) == collapse_ws(raw):
        return None
    return out


@dataclass(frozen=True)
class SeoProposal:
    seo_title: str
    seo_description: str
    image_alt: str
    body_html_stripped: str | None
    change_seo_title: bool
    change_seo_description: bool
    change_image_alt: bool
    change_body_homnn: bool
    change_barcode: bool
    proposed_barcode: str


def build_proposal(
    *,
    title: str,
    vendor: str,
    body_html: str,
    ymm_summary: str,
    current_seo_title: str,
    current_seo_description: str,
    current_alts: list[str],
    current_barcode: str,
    source_barcode: str,
) -> SeoProposal:
    seo_title = propose_seo_title(title, vendor)
    seo_desc = propose_seo_description(
        title=title, vendor=vendor, body_html=body_html, ymm_summary=ymm_summary
    )
    alt = propose_image_alt(title, vendor)
    src_bc = re.sub(r"\D+", "", source_barcode or "")
    cur_bc = (current_barcode or "").strip()
    change_barcode = bool(src_bc) and not cur_bc
    empty_alts = bool(current_alts) and all(not (a or "").strip() for a in current_alts)
    return SeoProposal(
        seo_title=seo_title,
        seo_description=seo_desc,
        image_alt=alt,
        body_html_stripped=None,
        change_seo_title=not (current_seo_title or "").strip(),
        change_seo_description=not (current_seo_description or "").strip(),
        change_image_alt=empty_alts,
        change_body_homnn=False,
        change_barcode=change_barcode,
        proposed_barcode=src_bc if change_barcode else cur_bc,
    )
