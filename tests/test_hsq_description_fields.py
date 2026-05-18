"""HSQ/HQV: alle bekende TEXTART-bronnen voor Body (HTML)."""

from __future__ import annotations

from lxml import etree

import config
from modules.xml_loader import build_description


def _elem_with_textart(name: str, text: str) -> etree._Element:
    xml = f"""
    <PRODUKT name="SKU1">
      <TEXTART name="{name}">
        <TEXT culture="EN-GB">{text}</TEXT>
      </TEXTART>
    </PRODUKT>
    """
    return etree.fromstring(xml)


def test_hsq_uses_marketingbez_hqv() -> None:
    config.apply_brand("hsq")
    elem = _elem_with_textart("MARKETINGBEZ_HQV", "Official Husqvarna marketing text")
    assert build_description(elem) == "Official Husqvarna marketing text"


def test_hsq_preishinweis_wrapped_in_paragraph() -> None:
    config.apply_brand("hsq")
    elem = _elem_with_textart("PREISHINWEIS", "Price on request")
    assert build_description(elem) == "<p>Price on request</p>"


def test_hsq_falls_back_to_bezeichnung() -> None:
    config.apply_brand("hsq")
    elem = _elem_with_textart("BEZEICHNUNG", "Oil filter")
    assert build_description(elem) == "Oil filter"


def test_wp_uses_wp_langtext() -> None:
    config.apply_brand("wp")
    from lxml import etree

    xml = """
    <STRUKTUR_ELEMENT name="X">
      <TEXTART name="WP_LANGTEXT">
        <TEXT culture="EN-GB">WP fork description</TEXT>
      </TEXTART>
    </STRUKTUR_ELEMENT>
    """
    elem = etree.fromstring(xml)
    from modules.xml_loader import build_description

    assert build_description(elem) == "WP fork description"


def test_ktm_does_not_use_bezeichnung_as_body() -> None:
    config.apply_brand("ktm")
    elem = _elem_with_textart("BEZEICHNUNG", "Oil filter")
    assert build_description(elem) == ""
