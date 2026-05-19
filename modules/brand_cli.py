"""Gedeelde --brand / -b CLI voor scripts (zelfde merken als main.py)."""

from __future__ import annotations

import argparse
import os

from modules.brand_config import DEFAULT_BRAND_ID, VALID_BRAND_IDS


def bootstrap_brand_from_argv() -> str:
    """
    Parse --brand uit sys.argv vóór ``import config`` en zet ``BRAND`` in de omgeving.
    Default: ktm (ongewijzigd legacy-gedrag).
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--brand",
        "-b",
        default=os.environ.get("BRAND", DEFAULT_BRAND_ID),
        choices=tuple(sorted(VALID_BRAND_IDS)),
        help=argparse.SUPPRESS,
    )
    args, _unknown = parser.parse_known_args()
    brand = (args.brand or DEFAULT_BRAND_ID).strip().lower()
    os.environ["BRAND"] = brand
    return brand


def add_brand_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--brand",
        "-b",
        default=os.environ.get("BRAND", DEFAULT_BRAND_ID),
        choices=tuple(sorted(VALID_BRAND_IDS)),
        help="Merk: ktm (default), hsq (Husqvarna), wp.",
    )


def apply_parsed_brand(brand: str | None) -> str:
    """Herlaad config-paden na argparse (import config moet al geladen zijn)."""
    import config

    config.apply_brand(brand)
    return config.BRAND_ID
