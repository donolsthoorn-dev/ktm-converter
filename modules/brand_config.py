"""Brand-definities voor KTM / Husqvarna (hsq) / WP — zelfde ETL, andere paden en handle-prefix."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_BRAND_ID = "ktm"
VALID_BRAND_IDS = frozenset({"ktm", "hsq", "wp"})


@dataclass(frozen=True)
class BrandConfig:
    id: str
    handle_prefix: str
    shopify_vendor: str
    # Prefix voor kolommen Type en Tags in product-CSV (bv. "HSQ - ")
    shopify_type_tag_prefix: str
    input_dir: str
    base_output_dir: str
    xml_glob: str
    xml_env_var: str
    xml_fallback_name: str
    # TEXTART-namen voor Body (HTML); volgorde = voorkeur (eerste hit wint)
    description_textart_names: tuple[str, ...]
    description_features_textart_names: tuple[str, ...]
    # Korte teksten / hints → <p>…</p> als het geen HTML is
    description_paragraph_textart_names: tuple[str, ...]
    # Laatste redmiddel (o.a. BEZEICHNUNG); alleen hsq/wp — KTM leeg laten
    description_fallback_textart_names: tuple[str, ...]
    price_csv_names: tuple[str, ...]
    price_csv_fallback_globs: tuple[str, ...]
    price_csv_env_var: str
    log_prefix: str


_BRANDS: dict[str, BrandConfig] = {
    "ktm": BrandConfig(
        id="ktm",
        handle_prefix="",
        shopify_vendor="KTM",
        shopify_type_tag_prefix="",
        input_dir="input",
        base_output_dir="output",
        xml_glob="CBEXPDN_KTM-DN*.xml",
        xml_env_var="KTM_XML_FILE",
        xml_fallback_name="CBEXPDN_KTM-DN-3008-0.xml",
        description_textart_names=(
            "BESCHRTEXT_ALG",
            "BESCHRTEXT_GEN_D",
            "BESCHRTEXT_GEN",
        ),
        description_features_textart_names=("BESCHRTEXT_EIGENSCH",),
        description_paragraph_textart_names=(),
        description_fallback_textart_names=(),
        price_csv_names=("0150_35_Z1_EUR_EN_csv.csv",),
        price_csv_fallback_globs=("*0150*.csv",),
        price_csv_env_var="KTM_0150_CSV",
        log_prefix="ktm",
    ),
    "hsq": BrandConfig(
        id="hsq",
        handle_prefix="hsq-",
        shopify_vendor="HUSQVARNA",
        shopify_type_tag_prefix="HSQ - ",
        input_dir="input/hsq",
        base_output_dir="output/hsq",
        xml_glob="CBEXPDN*.xml",
        xml_env_var="HSQ_XML_FILE",
        xml_fallback_name="CBEXPDN_HQV-DN-3008-0.xml",
        description_textart_names=(
            "BESCHRTEXT_ALG_HQV",
            "BESCHRTEXT_GEN_D_HQV",
            "BESCHRTEXT_GEN_W_HQV",
            "BESCHRTEXT_ALG",
            "BESCHRTEXT_GEN_D",
            "BESCHRTEXT_GEN",
            "MARKETINGBEZ_HQV",
            "MARKETINGBEZ",
            "SLOGAN",
        ),
        description_features_textart_names=(
            "BESCHR_EIGENSCH_HQV",
            "BESCHRTEXT_EIGENSCH",
        ),
        description_paragraph_textart_names=(
            "PREISHINWEIS",
            "PREISHINWEIS_ZUS",
            "FINANZIERUNGSHINWEIS",
        ),
        description_fallback_textart_names=(
            "MODELLNAME_GEN_HQV",
            "MODELLNAME_HQV",
            "BEZEICHNUNG",
        ),
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
        shopify_vendor="WP",
        shopify_type_tag_prefix="WP - ",
        input_dir="input/wp",
        base_output_dir="output/wp",
        xml_glob="CBEXPDN*.xml",
        xml_env_var="WP_XML_FILE",
        xml_fallback_name="CBEXPDN_WP-DN-2580-0.xml",
        # WP PIM: langtekst op structuur (WP_LANGTEXT), marketing op PRODUKT (MARKETINGBEZ)
        description_textart_names=(
            "WP_LANGTEXT",
            "BESCHRTEXT_ALG",
            "BESCHRTEXT_GEN_D",
            "BESCHRTEXT_GEN",
            "MARKETINGBEZ",
            "SLOGAN",
        ),
        description_features_textart_names=("BESCHRTEXT_EIGENSCH",),
        description_paragraph_textart_names=(
            "PREISHINWEIS",
            "PREISHINWEIS_ZUS",
        ),
        description_fallback_textart_names=(
            "MODELLNAME_GEN",
            "MODELLNAME",
            "BEZEICHNUNG",
        ),
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

# Volgorde voor multi-brand prijs/ETA-sync: later bestand wint bij dubbele SKU.
PRICELIST_CSV_MERGE_ORDER: tuple[str, ...] = (
    "1100_35_Z1_EUR_EN_csv.csv",
    "0910_35_Z1_EUR_EN_csv.csv",
    "0150_35_Z1_EUR_EN_csv.csv",
    "0140_35_Z1_EUR_EN_csv.csv",
)


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


def pricelist_csv_relative_path(filename: str) -> str:
    """Verwacht pad t.o.v. projectroot (zoals na prepare_input_from_ftp)."""
    route = FTP_FILE_ROUTES.get(filename.strip(), "input")
    return f"{route}/{filename.strip()}"


def resolve_pricelist_csv_path(project_root: Path, filename: str) -> Path | None:
    """Zoek één prijs-CSV (merkmap + legacy flat copy in input/)."""
    name = filename.strip()
    route = FTP_FILE_ROUTES.get(name, "input")
    candidates = [
        project_root / route / name,
        project_root / "input" / name,
    ]
    seen: set[Path] = set()
    for p in candidates:
        key = p.resolve()
        if key in seen:
            continue
        seen.add(key)
        if p.is_file() and p.stat().st_size > 0:
            return key
    return None


def resolve_pricelist_csv_paths(
    project_root: Path,
    explicit: list[str] | None = None,
) -> list[Path]:
    """
    Paden voor merge van alle merk-prijs-CSV's.
    Zonder explicit: standaardset in PRICELIST_CSV_MERGE_ORDER (ontbrekende = waarschuwing).
    Met explicit: elk pad moet bestaan (ook in input/hsq of input/wp).
    """
    if explicit:
        out: list[Path] = []
        for s in explicit:
            p = Path(s)
            if p.is_file():
                out.append(p.resolve())
                continue
            name = p.name
            found = resolve_pricelist_csv_path(project_root, name)
            if found is not None:
                out.append(found)
                continue
            legacy = project_root / "input" / s
            if legacy.is_file():
                out.append(legacy.resolve())
                continue
            raise FileNotFoundError(f"CSV niet gevonden: {s}")
        return out

    found: list[Path] = []
    missing: list[str] = []
    for name in PRICELIST_CSV_MERGE_ORDER:
        p = resolve_pricelist_csv_path(project_root, name)
        if p is not None:
            found.append(p)
        else:
            missing.append(pricelist_csv_relative_path(name))
    if found:
        if missing:
            print(
                "Waarschuwing: deze prijs-CSV's ontbreken en worden overgeslagen: "
                + ", ".join(missing),
                flush=True,
            )
        return found
    return []


def missing_pricelist_csv_paths(project_root: Path) -> list[str]:
    """Relatieve paden die ontbreken of leeg zijn (voor CI-validatie)."""
    missing: list[str] = []
    for name in PRICELIST_CSV_MERGE_ORDER:
        if resolve_pricelist_csv_path(project_root, name) is None:
            missing.append(pricelist_csv_relative_path(name))
    return missing
