#!/usr/bin/env python3
"""Sync files from FTP staging folder to input folder."""

from __future__ import annotations

import argparse
import fnmatch
import os
import shutil
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402
from modules.brand_config import FTP_FILE_ROUTES, route_dir_for_filename  # noqa: E402
from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _parse_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    for item in raw.replace("\n", ",").split(","):
        s = item.strip()
        if s:
            out.append(s)
    return out


def _copy_or_move(src: Path, dst: Path, move: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if move:
        if dst.exists():
            dst.unlink()
        shutil.move(str(src), str(dst))
    else:
        shutil.copy2(src, dst)


def _same_file(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return False


def _zip_member_relpath(member_filename: str) -> Path:
    """Normaliseer zip-pad (geen absolute/escape paths)."""
    parts = [p for p in Path(member_filename).parts if p not in ("", ".", "..")]
    return Path(*parts) if parts else Path(Path(member_filename).name)


def _is_media_member(rel: Path) -> bool:
    """Alleen bestanden onder een PHO/-map in de zip (geen losse jpg’s elders)."""
    parts_l = [p.lower() for p in rel.parts]
    if "pho" not in parts_l:
        return False
    if rel.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".gif"}:
        return False
    return "fallback_no_picture" not in rel.name.lower()


def _extract_xml_from_zip(zip_path: Path, input_dir: Path, dry_run: bool) -> int:
    count = 0
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        for member in members:
            name = Path(member.filename).name
            if not name.lower().endswith(".xml"):
                continue
            dest = input_dir / name
            if dry_run:
                print(f"[dry-run] extract {zip_path.name}:{member.filename} -> {dest}")
            else:
                with zf.open(member, "r") as src, open(dest, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                print(f"Extracted: {zip_path.name}:{member.filename} -> {dest}")
            count += 1
    return count


def _extract_media_from_zip(zip_path: Path, input_dir: Path, dry_run: bool) -> int:
    """Pak PHO/media uit zip naar input_dir, behoud relatieve pad onder PHO/…"""
    count = 0
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            rel = _zip_member_relpath(member.filename)
            if not _is_media_member(rel):
                continue
            # Altijd onder input_dir; strip leading junk tot we bij PHO of file zijn
            parts_l = [p.lower() for p in rel.parts]
            if "pho" in parts_l:
                idx = parts_l.index("pho")
                rel = Path(*rel.parts[idx:])
            dest = input_dir / rel
            if dry_run:
                print(f"[dry-run] media {zip_path.name}:{member.filename} -> {dest}")
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member, "r") as src, open(dest, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                if count < 5 or count % 500 == 0:
                    print(f"Media: {dest.relative_to(input_dir)}", flush=True)
            count += 1
    return count


def _prune_old_cbexpdn_xml(input_dir: Path, keep_names: set[str], dry_run: bool) -> int:
    """Verwijder oude CBEXPDN_*.xml behalve zojuist geëxtraheerde."""
    removed = 0
    for path in sorted(input_dir.glob("CBEXPDN_*.xml")):
        if path.name in keep_names:
            continue
        if dry_run:
            print(f"[dry-run] prune XML {path}")
        else:
            path.unlink()
            print(f"Oude XML verwijderd: {path.name}")
        removed += 1
    return removed


def _remove_unpacked_zip(zip_path: Path, extra_copies: list[Path], dry_run: bool) -> None:
    seen: set[Path] = set()
    for path in [zip_path, *extra_copies]:
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key in seen or not path.is_file():
            continue
        seen.add(key)
        if dry_run:
            print(f"[dry-run] verwijder {path}")
            continue
        path.unlink()
        print(f"Verwijderd: {path}")


def _build_default_file_list() -> list[str]:
    # If no explicit prepare list is set, reuse the FTP download list.
    return _parse_list(_env("KTM_PREPARE_FILES")) or _parse_list(_env("KTM_SFTP_FILES"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--staging",
        default=_env("KTM_SFTP_LOCAL_DIR", "downloads/ftp"),
        help="Bronmap met opgehaalde FTP-bestanden.",
    )
    parser.add_argument(
        "--input-dir",
        default=config.INPUT_DIR,
        help=(
            f"Standaard doelmap (default: {config.INPUT_DIR}). "
            f"Prijs-CSV's worden gerouteerd: {', '.join(f'{k}→{v}' for k, v in sorted(FTP_FILE_ROUTES.items()))}."
        ),
    )
    parser.add_argument(
        "--files",
        default=None,
        help="Komma-gescheiden bestandsnamen (default: KTM_PREPARE_FILES of KTM_SFTP_FILES).",
    )
    parser.add_argument(
        "--pattern",
        default=None,
        help='Neem bestanden op basis van patroon, bv "*.csv" (alleen als --files leeg is).',
    )
    parser.add_argument(
        "--move",
        action="store_true",
        help="Verplaats bestanden i.p.v. kopieren.",
    )
    parser.add_argument(
        "--extract-xml-from-zips",
        action="store_true",
        help=(
            "Pak XML uit zip-bestanden in de staging-map naar de doelmap. "
            "De zip wordt niet gekopieerd en na een geslaagde extractie verwijderd."
        ),
    )
    parser.add_argument(
        "--extract-media-from-zips",
        action="store_true",
        help=(
            "Pak ook PHO/foto’s uit de zip naar de doelmap (naast XML). "
            "Vereist --extract-xml-from-zips of werkt alleen op geselecteerde zips."
        ),
    )
    parser.add_argument(
        "--prune-old-xml",
        action="store_true",
        help="Na extract: oude CBEXPDN_*.xml in doelmap weg (behoud nieuwste uit deze run).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Toon acties zonder bestanden te schrijven.",
    )
    args = parser.parse_args()

    staging = Path(args.staging)
    input_dir = Path(args.input_dir)
    if not staging.exists():
        print(f"Staging map niet gevonden: {staging}")
        return 2
    if not args.dry_run:
        input_dir.mkdir(parents=True, exist_ok=True)

    file_list = _parse_list(args.files) or _build_default_file_list()
    selected: list[Path] = []
    if file_list:
        for name in file_list:
            p = staging / name
            if not p.exists():
                print(f"Niet gevonden in staging: {p}")
                continue
            selected.append(p)
    else:
        all_files = [p for p in staging.iterdir() if p.is_file()]
        pattern = args.pattern
        if pattern:
            selected = [p for p in all_files if fnmatch.fnmatch(p.name, pattern)]
        else:
            selected = all_files

    extract_zips = args.extract_xml_from_zips or args.extract_media_from_zips

    copied = 0
    for src in sorted(selected, key=lambda p: p.name.lower()):
        if extract_zips and src.suffix.lower() == ".zip":
            continue
        route = route_dir_for_filename(src.name)
        dest_dir = Path(route) if route else input_dir
        dst = dest_dir / src.name
        if _same_file(src, dst):
            print(f"Al op de plek: {dst}")
            copied += 1
            continue
        if args.dry_run:
            action = "move" if args.move else "copy"
            print(f"[dry-run] {action} {src} -> {dst}")
        else:
            _copy_or_move(src, dst, move=args.move)
            print(f"Prepared: {src.name} -> {dst}")
        copied += 1

    extracted_xml = 0
    extracted_media = 0
    kept_xml: set[str] = set()
    if extract_zips:
        zip_sources = [p for p in selected if p.suffix.lower() == ".zip"]
        for zp in zip_sources:
            n_xml = 0
            n_media = 0
            try:
                with zipfile.ZipFile(zp) as zf:
                    for m in zf.infolist():
                        name = Path(m.filename).name
                        if name.lower().endswith(".xml"):
                            kept_xml.add(name)
                if args.extract_xml_from_zips or args.extract_media_from_zips:
                    n_xml = _extract_xml_from_zip(zp, input_dir, args.dry_run)
                    extracted_xml += n_xml
                if args.extract_media_from_zips:
                    n_media = _extract_media_from_zip(zp, input_dir, args.dry_run)
                    extracted_media += n_media
            except (zipfile.BadZipFile, OSError) as exc:
                print(f"Uitpakken mislukt, zip blijft staan: {zp} ({exc})")
                return 1
            if n_xml == 0 and n_media == 0:
                print(f"Niets bruikbaars in {zp.name}; zip blijft staan.")
                continue
            route = route_dir_for_filename(zp.name)
            dest_dir = Path(route) if route else input_dir
            leftover = dest_dir / zp.name
            extras = [] if _same_file(leftover, zp) else [leftover]
            _remove_unpacked_zip(zp, extras, args.dry_run)

        if args.prune_old_xml and kept_xml:
            _prune_old_cbexpdn_xml(input_dir, kept_xml, args.dry_run)

    print(
        f"Klaar. Bestanden voorbereid: {copied}. "
        f"XML extracted: {extracted_xml}. Media extracted: {extracted_media}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
