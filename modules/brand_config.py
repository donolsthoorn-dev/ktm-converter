"""Brand-definities voor KTM / Husqvarna (hsq) / WP — zelfde ETL, andere paden en handle-prefix."""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_BRAND_ID = "ktm"
VALID_BRAND_IDS = frozenset({"ktm", "hsq", "wp"})


@dataclass(frozen=True)
class BrandConfig:
    id: str
    handle_prefix: str
    input_dir: str
    base_output_dir: str
    xml_glob: str
    xml_env_var: str
    xml_fallback_name: str
    price_csv_names: tuple[str, ...]
    price_csv_fallback_globs: tuple[str, ...]
    price_csv_env_var: str
    log_prefix: str


_BRANDS: dict[str, BrandConfig] = {
    "ktm": BrandConfig(
        id="ktm",
        handle_prefix="",
        input_dir="input",
        base_output_dir="output",
        xml_glob="CBEXPDN_KTM-DN*.xml",
        xml_env_var="KTM_XML_FILE",
        xml_fallback_name="CBEXPDN_KTM-DN-3008-0.xml",
        price_csv_names=("0150_35_Z1_EUR_EN_csv.csv",),
        price_csv_fallback_globs=("*0150*.csv",),
        price_csv_env_var="KTM_0150_CSV",
        log_prefix="ktm",
    ),
    "hsq": BrandConfig(
        id="hsq",
        handle_prefix="hsq-",
        input_dir="input/hsq",
        base_output_dir="output/hsq",
        xml_glob="CBEXPDN*.xml",
        xml_env_var="HSQ_XML_FILE",
        xml_fallback_name="CBEXPDN_HQV-DN-3008-0.xml",
        # 1100 eerst, 0140 overschrijft bij dubbele SKU
        price_csv_names=(
            "1100_35_Z1_EUR_EN_csv.csv",
            "0140_35_Z1_EUR_EN_csv.csv",
        ),
        price_csv_fallback_globs=("*1100*.csv", "*0140*.csv"),
        price_csv_env_var="HSQ_PRICE_CSV",
        log_prefix="hsq",
    ),
    "wp": BrandConfig(
        id="wp",
        handle_prefix="wp-",
        input_dir="input/wp",
        base_output_dir="output/wp",
        xml_glob="CBEXPDN*.xml",
        xml_env_var="WP_XML_FILE",
        xml_fallback_name="CBEXPDN_WP-DN-3008-0.xml",
        price_csv_names=("0910_35_Z1_EUR_EN_csv.csv",),
        price_csv_fallback_globs=("*0910*.csv",),
        price_csv_env_var="WP_PRICE_CSV",
        log_prefix="wp",
    ),
}

# FTP/prepare: bestandsnaam → doelmap onder projectroot
FTP_FILE_ROUTES: dict[str, str] = {
    "0150_35_Z1_EUR_EN_csv.csv": "input",
    "0140_35_Z1_EUR_EN_csv.csv": "input/hsq",
    "1100_35_Z1_EUR_EN_csv.csv": "input/hsq",
    "0910_35_Z1_EUR_EN_csv.csv": "input/wp",
}


def normalize_brand_id(raw: str | None) -> str:
    bid = (raw or DEFAULT_BRAND_ID).strip().lower()
    if bid not in VALID_BRAND_IDS:
        raise ValueError(
            f"Onbekend merk '{raw}'. Kies uit: {', '.join(sorted(VALID_BRAND_IDS))}."
        )
    return bid


def get_brand_config(brand_id: str | None = None) -> BrandConfig:
    bid = normalize_brand_id(brand_id)
    return _BRANDS[bid]


def route_dir_for_filename(filename: str) -> str | None:
    """Doelmap voor prepare_input_from_ftp (None = default --input-dir)."""
    return FTP_FILE_ROUTES.get(filename.strip())
