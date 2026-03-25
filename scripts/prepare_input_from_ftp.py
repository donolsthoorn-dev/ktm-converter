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
        help=f"Doelmap voor ETL input (default: {config.INPUT_DIR}).",
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
        help="Pak XML-bestanden uit zip-bestanden in de input-map.",
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

    copied = 0
    for src in sorted(selected, key=lambda p: p.name.lower()):
        dst = input_dir / src.name
        if args.dry_run:
            action = "move" if args.move else "copy"
            print(f"[dry-run] {action} {src} -> {dst}")
        else:
            _copy_or_move(src, dst, move=args.move)
            print(f"Prepared: {src.name} -> {dst}")
        copied += 1

    extracted = 0
    if args.extract_xml_from_zips:
        zip_sources = [p for p in selected if p.suffix.lower() == ".zip"]
        for zp in zip_sources:
            extracted += _extract_xml_from_zip(zp, input_dir, args.dry_run)

    print(f"Klaar. Bestanden voorbereid: {copied}. XML extracted uit zips: {extracted}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
