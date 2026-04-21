from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

_HS_ALLOWED_LENGTHS_DEFAULT = (6, 8, 10)
_COUNTRY_ALIASES = {
    "AT": "AT",
    "AUSTRIA": "AT",
    "AUT": "AT",
    "BE": "BE",
    "BELGIUM": "BE",
    "BEL": "BE",
    "CH": "CH",
    "SWITZERLAND": "CH",
    "CHE": "CH",
    "CN": "CN",
    "CHINA": "CN",
    "CHN": "CN",
    "CZ": "CZ",
    "CZECH REPUBLIC": "CZ",
    "CZE": "CZ",
    "DE": "DE",
    "GERMANY": "DE",
    "DEU": "DE",
    "DK": "DK",
    "DENMARK": "DK",
    "DNK": "DK",
    "ES": "ES",
    "SPAIN": "ES",
    "ESP": "ES",
    "FI": "FI",
    "FINLAND": "FI",
    "FIN": "FI",
    "FR": "FR",
    "FRANCE": "FR",
    "FRA": "FR",
    "GB": "GB",
    "UK": "GB",
    "UNITED KINGDOM": "GB",
    "GREAT BRITAIN": "GB",
    "GBR": "GB",
    "HU": "HU",
    "HUNGARY": "HU",
    "HUN": "HU",
    "IT": "IT",
    "ITALY": "IT",
    "ITA": "IT",
    "JP": "JP",
    "JAPAN": "JP",
    "JPN": "JP",
    "NL": "NL",
    "NETHERLANDS": "NL",
    "THE NETHERLANDS": "NL",
    "NLD": "NL",
    "NO": "NO",
    "NORWAY": "NO",
    "NOR": "NO",
    "PL": "PL",
    "POLAND": "PL",
    "POL": "PL",
    "PT": "PT",
    "PORTUGAL": "PT",
    "PRT": "PT",
    "RO": "RO",
    "ROMANIA": "RO",
    "ROU": "RO",
    "SE": "SE",
    "SWEDEN": "SE",
    "SWE": "SE",
    "SI": "SI",
    "SLOVENIA": "SI",
    "SVN": "SI",
    "SK": "SK",
    "SLOVAKIA": "SK",
    "SVK": "SK",
    "TH": "TH",
    "THAILAND": "TH",
    "THA": "TH",
    "TR": "TR",
    "TURKEY": "TR",
    "TUR": "TR",
    "TW": "TW",
    "TAIWAN": "TW",
    "TWN": "TW",
    "US": "US",
    "USA": "US",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "VN": "VN",
    "VIETNAM": "VN",
    "VNM": "VN",
}


def parse_allowed_hs_lengths(raw: str | None) -> tuple[int, ...]:
    if not raw:
        return _HS_ALLOWED_LENGTHS_DEFAULT
    out: list[int] = []
    for token in str(raw).replace(";", ",").split(","):
        s = token.strip()
        if not s:
            continue
        if not s.isdigit():
            continue
        n = int(s)
        if n > 0:
            out.append(n)
    if not out:
        return _HS_ALLOWED_LENGTHS_DEFAULT
    return tuple(sorted(set(out)))


def normalize_hs_code(raw: Any, allowed_lengths: tuple[int, ...] | None = None) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    lens = allowed_lengths or _HS_ALLOWED_LENGTHS_DEFAULT
    if len(digits) not in set(lens):
        return None
    return digits


def normalize_country_code(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    key = re.sub(r"\s+", " ", s).upper()
    if key in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[key]
    if len(key) == 2 and key.isalpha():
        return key
    return None


def _first_matching_key(row: dict[str, str], candidates: tuple[str, ...]) -> str:
    norm = {k.strip().lower(): k for k in row.keys()}
    for cand in candidates:
        if cand in norm:
            return norm[cand]
    return ""


def load_external_customs_map(
    csv_path: Path,
    allowed_hs_lengths: tuple[int, ...],
) -> tuple[dict[str, dict[str, str]], list[dict[str, str]], int]:
    out: dict[str, dict[str, str]] = {}
    rejected: list[dict[str, str]] = []
    rows_read = 0
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
            delim = dialect.delimiter
        except csv.Error:
            delim = ";"
        reader = csv.DictReader(fh, delimiter=delim)
        for row in reader:
            rows_read += 1
            sku_key = _first_matching_key(
                row,
                ("sku", "articlenumber", "article_number", "article", "variant_sku"),
            )
            hs_key = _first_matching_key(
                row,
                ("hs_code", "hscode", "customs_no", "harmonized_system_code", "customs"),
            )
            country_key = _first_matching_key(
                row,
                (
                    "country_of_origin",
                    "origin_country",
                    "countrycodeoforigin",
                    "country_code_of_origin",
                    "origin",
                ),
            )
            source_key = _first_matching_key(row, ("source", "origin_source"))

            sku = (row.get(sku_key) or "").strip().upper()
            if not sku:
                continue
            hs = normalize_hs_code(row.get(hs_key), allowed_hs_lengths) if hs_key else None
            country = normalize_country_code(row.get(country_key)) if country_key else None
            if not hs and not country:
                rejected.append(
                    {
                        "sku": sku,
                        "reason": "external_missing_or_invalid_customs_data",
                        "raw_hs": (row.get(hs_key) or "") if hs_key else "",
                        "raw_country": (row.get(country_key) or "") if country_key else "",
                    }
                )
                continue
            out[sku] = {
                "hs_code": hs or "",
                "country_of_origin": country or "",
                "source": (row.get(source_key) or "").strip() if source_key else "",
                "tier": "external_exact",
            }
    return out, rejected, rows_read


def load_xml_customs_map(
    xml_path: Path,
    allowed_hs_lengths: tuple[int, ...],
) -> tuple[dict[str, dict[str, str]], list[dict[str, str]], int]:
    out: dict[str, dict[str, str]] = {}
    rejected: list[dict[str, str]] = []
    sku_rows = 0
    for _event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag != "PRODUKT":
            continue
        sku_raw = elem.get("name") or ""
        sku = sku_raw.strip().upper()
        if not sku:
            elem.clear()
            continue
        sku_rows += 1
        attr_raw: dict[str, str] = {}
        for a in elem.findall(".//ATTRIBUTE/ATTRIBUT"):
            aname = (a.get("name") or "").strip().upper()
            if aname not in ("CUSTOMS_NO", "COUNTRY_OF_ORIGIN", "PTW_ORIGIN"):
                continue
            for aw in a.findall(".//ATTRIBUTWERT"):
                value = (aw.get("name") or "").strip()
                if value:
                    attr_raw[aname] = value
                    break
        hs = normalize_hs_code(attr_raw.get("CUSTOMS_NO"), allowed_hs_lengths)
        country = normalize_country_code(attr_raw.get("COUNTRY_OF_ORIGIN"))
        if not country:
            country = normalize_country_code(attr_raw.get("PTW_ORIGIN"))
        if hs or country:
            out[sku] = {
                "hs_code": hs or "",
                "country_of_origin": country or "",
                "source": "xml",
                "tier": "xml_exact",
            }
        elif attr_raw:
            rejected.append(
                {
                    "sku": sku,
                    "reason": "xml_customs_values_present_but_invalid",
                    "raw_hs": attr_raw.get("CUSTOMS_NO", ""),
                    "raw_country": attr_raw.get("COUNTRY_OF_ORIGIN", "") or attr_raw.get("PTW_ORIGIN", ""),
                }
            )
        elem.clear()
    return out, rejected, sku_rows


def merge_customs_sources(
    desired_skus: set[str],
    xml_map: dict[str, dict[str, str]],
    external_map: dict[str, dict[str, str]],
) -> tuple[dict[str, dict[str, str]], list[dict[str, str]]]:
    merged: dict[str, dict[str, str]] = {}
    report: list[dict[str, str]] = []
    for sku in sorted(desired_skus):
        x = xml_map.get(sku)
        e = external_map.get(sku)
        hs = ""
        country = ""
        tier = "missing"
        source = ""
        reason = "no_source_value"
        if x:
            hs = x.get("hs_code") or ""
            country = x.get("country_of_origin") or ""
            tier = "xml_exact"
            source = "xml"
            reason = "resolved_from_xml"
            if e:
                ext_hs = e.get("hs_code") or ""
                ext_country = e.get("country_of_origin") or ""
                if (ext_hs and hs and ext_hs != hs) or (
                    ext_country and country and ext_country != country
                ):
                    reason = "resolved_from_xml_conflict_external"
        elif e:
            hs = e.get("hs_code") or ""
            country = e.get("country_of_origin") or ""
            tier = "external_exact"
            source = e.get("source") or "external"
            reason = "resolved_from_external"

        if hs or country:
            merged[sku] = {
                "hs_code": hs,
                "country_of_origin": country,
                "tier": tier,
                "source": source,
            }

        report.append(
            {
                "sku": sku,
                "resolved": "1" if (hs or country) else "0",
                "hs_code": hs,
                "country_of_origin": country,
                "tier": tier,
                "source": source,
                "reason": reason,
            }
        )
    return merged, report
