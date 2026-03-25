#!/usr/bin/env python3
"""
Haal bestanden van remote FTP/SFTP binnen naar de lokale input-map.

Ondersteunde protocollen:
  - sftp
  - ftps (FTP over TLS, explicit)
  - ftp  (onversleuteld)

Configuratie via omgeving (zie .env.example):
  KTM_TRANSFER_PROTOCOL=ftps|sftp|ftp
  KTM_SFTP_HOST, KTM_SFTP_USER, KTM_SFTP_PASSWORD
Optioneel:
  KTM_SFTP_PORT (default: 21 voor ftp/ftps, 22 voor sftp)
  KTM_SFTP_REMOTE_DIR (/), KTM_SFTP_LOCAL_DIR (input)
  KTM_SFTP_FILES (komma- of regelgescheiden bestandsnamen)
  KTM_SFTP_PATTERN (glob, alleen als KTM_SFTP_FILES leeg is)
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import socket
import stat
import sys
import time
from ftplib import FTP, FTP_TLS
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

import config  # noqa: E402


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _protocol() -> str:
    p = _env("KTM_TRANSFER_PROTOCOL", "sftp").lower()
    if p not in ("sftp", "ftps", "ftp"):
        print("KTM_TRANSFER_PROTOCOL moet sftp, ftps of ftp zijn.", file=sys.stderr)
        raise SystemExit(2)
    return p


def _connect_sftp():
    try:
        import paramiko
    except ImportError as e:
        print(
            "paramiko ontbreekt voor SFTP. Installeer met: pip install -r requirements.txt",
            file=sys.stderr,
        )
        raise SystemExit(1) from e

    host = _env("KTM_SFTP_HOST")
    user = _env("KTM_SFTP_USER")
    if not host or not user:
        print(
            "Zet KTM_SFTP_HOST en KTM_SFTP_USER in .env (zie .env.example).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    port = int(_env("KTM_SFTP_PORT", "22"))
    password = _env("KTM_SFTP_PASSWORD")
    key_path = _env("KTM_SFTP_KEY_PATH")

    pkey = None
    if key_path:
        key_file = Path(key_path).expanduser()
        if not key_file.is_file():
            print(f"Key-bestand niet gevonden: {key_file}", file=sys.stderr)
            raise SystemExit(2)
        # Probeer gangbare key-typen
        key_pass = _env("KTM_SFTP_KEY_PASSPHRASE") or None
        for loader in (
            paramiko.Ed25519Key,
            paramiko.RSAKey,
            paramiko.ECDSAKey,
        ):
            try:
                pkey = loader.from_private_key_file(str(key_file), password=key_pass)
                break
            except paramiko.SSHException:
                continue
        if pkey is None:
            print("Kon private key niet laden (formaat?).", file=sys.stderr)
            raise SystemExit(2)
    elif not password:
        print(
            "Zet KTM_SFTP_PASSWORD of KTM_SFTP_KEY_PATH in .env.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=user,
        password=password or None,
        pkey=pkey,
        allow_agent=False,
        look_for_keys=False,
    )
    return client, client.open_sftp()


def _connect_ftp(use_tls: bool):
    host = _env("KTM_SFTP_HOST")
    user = _env("KTM_SFTP_USER")
    password = _env("KTM_SFTP_PASSWORD")
    if not host or not user or not password:
        print(
            "Zet KTM_SFTP_HOST, KTM_SFTP_USER en KTM_SFTP_PASSWORD in .env.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    default_port = "21"
    port = int(_env("KTM_SFTP_PORT", default_port))
    ftp = FTP_TLS() if use_tls else FTP()
    timeout_sec = float(_env("KTM_FTP_TIMEOUT_SEC", "120") or "120")
    ftp.connect(host=host, port=port, timeout=timeout_sec)
    ftp.login(user=user, passwd=password)
    if use_tls:
        # Beveilig datakanaal (zoals FileZilla: FTP over TLS)
        ftp.prot_p()
    return ftp


def _close_ftp_quietly(ftp) -> None:
    try:
        ftp.quit()
    except Exception:
        try:
            ftp.close()
        except Exception:
            pass


def _download_ftp_file_with_resume(
    ftp,
    remote_dir: str,
    name: str,
    dest: Path,
    use_tls: bool,
) -> None:
    retries = int(_env("KTM_FTP_RETRIES", "6") or "6")
    sleep_sec = float(_env("KTM_FTP_RETRY_SLEEP_SEC", "3") or "3")
    blocksize = int(_env("KTM_FTP_BLOCKSIZE", "1048576") or "1048576")
    attempt = 0
    while True:
        attempt += 1
        offset = dest.stat().st_size if dest.exists() else 0
        mode = "ab" if offset > 0 else "wb"
        try:
            with open(dest, mode) as f:

                def _write(chunk: bytes) -> None:
                    f.write(chunk)

                ftp.retrbinary(
                    f"RETR {name}", _write, blocksize=blocksize, rest=offset if offset > 0 else None
                )
            return
        except (TimeoutError, socket.timeout, OSError) as e:
            if attempt > retries:
                raise RuntimeError(f"Download gaf timeout/fout na {retries} retries: {name}") from e
            print(
                f"Waarschuwing: download onderbroken voor {name} "
                f"(attempt {attempt}/{retries}, resume vanaf {offset} bytes).",
                file=sys.stderr,
            )
            time.sleep(sleep_sec)
            _close_ftp_quietly(ftp)
            ftp = _connect_ftp(use_tls=use_tls)
            ftp.cwd(remote_dir)


def _norm_remote(path: str) -> str:
    p = path.replace("\\", "/").strip()
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/") or "/"


def _join_remote(remote_dir: str, name: str) -> str:
    base = remote_dir.rstrip("/") or "/"
    if base == "/":
        return "/" + name
    return base + "/" + name


def _display_remote_file(remote_dir: str, name: str) -> str:
    return _join_remote(remote_dir, name)


def _skip_if_exists(dest: Path) -> bool:
    if dest.exists():
        print(f"Overslaan (bestaat al): {dest}")
        return True
    return False


def _list_remote_sftp(sftp, remote_dir: str) -> None:
    rd = _norm_remote(remote_dir)
    print(f"Inhoud van {rd}:")
    for attr in sorted(sftp.listdir_attr(rd), key=lambda a: a.filename):
        mode = "d" if stat.S_ISDIR(attr.st_mode) else "-"
        size = attr.st_size if not stat.S_ISDIR(attr.st_mode) else 0
        print(f"  {mode} {attr.filename:50} {size:>12}")


def _parse_file_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        s = chunk.strip()
        if s:
            out.append(s)
    return out


def _should_take(name: str, pattern: str | None) -> bool:
    if not pattern:
        return True
    return fnmatch.fnmatch(name, pattern)


def _download_tree_sftp(
    sftp,
    remote_dir: str,
    local_dir: Path,
    pattern: str | None,
    recursive: bool,
    dry_run: bool,
) -> tuple[int, int]:
    """Returns (files_copied, bytes_total)."""
    rd = _norm_remote(remote_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    n_files = 0
    n_bytes = 0
    for attr in sftp.listdir_attr(rd):
        name = attr.filename
        if name in (".", ".."):
            continue
        rpath = _join_remote(rd, name)
        if stat.S_ISDIR(attr.st_mode):
            if recursive:
                sub = _download_tree_sftp(
                    sftp,
                    rpath,
                    local_dir / name,
                    pattern,
                    recursive,
                    dry_run,
                )
                n_files += sub[0]
                n_bytes += sub[1]
            continue
        if not _should_take(name, pattern):
            continue
        dest = local_dir / name
        if _skip_if_exists(dest):
            continue
        size = getattr(attr, "st_size", 0) or 0
        if dry_run:
            print(f"[dry-run] zou ophalen: {rpath} -> {dest} ({size} bytes)")
        else:
            print(f"Ophalen: {rpath} -> {dest}")
            sftp.get(rpath, str(dest))
            n_bytes += size
        n_files += 1
    return n_files, n_bytes


def _download_named_sftp(
    sftp,
    remote_dir: str,
    local_dir: Path,
    names: list[str],
    dry_run: bool,
) -> tuple[int, int]:
    """Download exacte bestandsnamen onder remote_dir (geen wildcards)."""
    rd = _norm_remote(remote_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    n_files = 0
    n_bytes = 0
    for name in names:
        rpath = _join_remote(rd, name)
        dest = local_dir / name
        if _skip_if_exists(dest):
            continue
        try:
            st = sftp.stat(rpath)
        except OSError as e:
            print(f"Niet gevonden op server: {rpath} ({e})", file=sys.stderr)
            continue
        if stat.S_ISDIR(st.st_mode):
            print(f"Overslaan (is een map): {rpath}", file=sys.stderr)
            continue
        size = int(getattr(st, "st_size", 0) or 0)
        if dry_run:
            print(f"[dry-run] zou ophalen: {rpath} -> {dest} ({size} bytes)")
        else:
            print(f"Ophalen: {rpath} -> {dest}")
            sftp.get(rpath, str(dest))
            n_bytes += size
        n_files += 1
    return n_files, n_bytes


def main() -> int:
    ap = argparse.ArgumentParser(description="FTP/SFTP: bestanden naar lokale input-map.")
    ap.add_argument(
        "--remote",
        default=_env("KTM_SFTP_REMOTE_DIR", "/"),
        help="Remote map (default: KTM_SFTP_REMOTE_DIR of /).",
    )
    ap.add_argument(
        "--local",
        default=_env("KTM_SFTP_LOCAL_DIR") or config.INPUT_DIR,
        help=f"Lokale map (default: KTM_SFTP_LOCAL_DIR of {config.INPUT_DIR}).",
    )
    ap.add_argument(
        "--pattern",
        default=_env("KTM_SFTP_PATTERN") or None,
        help='Alleen bestandsnamen die hierop matchen (bijv. "*.xml").',
    )
    ap.add_argument(
        "--recursive",
        action="store_true",
        help="Ook submappen downloaden (behoudt structuur onder --local).",
    )
    ap.add_argument(
        "--list",
        action="store_true",
        help="Alleen remote map tonen, niets downloaden.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Toon wat zou worden opgehaald zonder te schrijven.",
    )
    ap.add_argument(
        "--files",
        default=None,
        metavar="NAMEN",
        help="Komma-gescheiden bestandsnamen (alleen deze). Overschrijft KTM_SFTP_FILES.",
    )
    args = ap.parse_args()

    file_list = _parse_file_list(args.files) or _parse_file_list(_env("KTM_SFTP_FILES"))

    protocol = _protocol()
    remote_dir = _norm_remote(args.remote)
    local_dir = Path(args.local)
    local_dir.mkdir(parents=True, exist_ok=True)

    if protocol == "sftp":
        client, sftp = _connect_sftp()
        try:
            if args.list:
                _list_remote_sftp(sftp, remote_dir)
                return 0
            if file_list:
                n_files, n_bytes = _download_named_sftp(
                    sftp, remote_dir, local_dir, file_list, args.dry_run
                )
            else:
                n_files, n_bytes = _download_tree_sftp(
                    sftp,
                    remote_dir,
                    local_dir,
                    args.pattern,
                    args.recursive,
                    args.dry_run,
                )
        finally:
            sftp.close()
            client.close()
    else:
        use_tls = protocol == "ftps"
        ftp = _connect_ftp(use_tls=use_tls)
        try:
            ftp.cwd(remote_dir)
            if args.list:
                print(f"Inhoud van {remote_dir}:")
                names = sorted(ftp.nlst())
                for name in names:
                    if name in (".", ".."):
                        continue
                    base = name.split("/")[-1]
                    size = 0
                    try:
                        size_val = ftp.size(base)
                        size = int(size_val) if size_val is not None else 0
                    except Exception:
                        pass
                    print(f"  - {base:50} {size:>12}")
                return 0
            if args.recursive:
                print(
                    "--recursive wordt op FTP/FTPS nu niet ondersteund.",
                    file=sys.stderr,
                )
                return 2
            names = sorted([n.split("/")[-1] for n in ftp.nlst() if n not in (".", "..")])
            if file_list:
                wanted = file_list
            else:
                wanted = [n for n in names if _should_take(n, args.pattern)]
            n_files = 0
            n_bytes = 0
            for name in wanted:
                if name not in names:
                    print(
                        f"Niet gevonden op server: {_display_remote_file(remote_dir, name)}",
                        file=sys.stderr,
                    )
                    continue
                dest = local_dir / name
                if _skip_if_exists(dest):
                    continue
                size = 0
                try:
                    size_val = ftp.size(name)
                    size = int(size_val) if size_val is not None else 0
                except Exception:
                    pass
                if args.dry_run:
                    print(
                        f"[dry-run] zou ophalen: {_display_remote_file(remote_dir, name)} -> {dest} ({size} bytes)"
                    )
                else:
                    print(f"Ophalen: {_display_remote_file(remote_dir, name)} -> {dest}")
                    _download_ftp_file_with_resume(
                        ftp,
                        remote_dir=remote_dir,
                        name=name,
                        dest=dest,
                        use_tls=use_tls,
                    )
                    n_bytes += size
                n_files += 1
        finally:
            _close_ftp_quietly(ftp)

    if args.dry_run:
        print(f"[dry-run] {n_files} bestand(en).")
    else:
        print(f"Klaar: {n_files} bestand(en), {n_bytes} bytes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
