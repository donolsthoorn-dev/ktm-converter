"""CSV helpers: write + size-based chunk split (header preserved)."""

from __future__ import annotations

import csv
import logging
import shutil
from pathlib import Path

log = logging.getLogger("ebihr.csv_util")


def write_csv(path: Path, header: list[str], rows: list[list]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)
    log.info("Geschreven %s (%d rijen, %.1f MB)", path.name, len(rows), path.stat().st_size / 1e6)
    return path


def split_csv_by_bytes(
    path: Path,
    *,
    max_bytes: int,
    chunks_dir: Path | None = None,
) -> list[Path]:
    """
    Split CSV into part files under ``{stem}_chunks/`` when over max_bytes.
    Returns list of part paths (or [path] if under limit).
    """
    size = path.stat().st_size
    if size <= max_bytes:
        return [path]

    out_dir = chunks_dir or path.with_name(path.stem + "_chunks")
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    parts: list[Path] = []
    part_idx = 0
    out_f = None
    writer = None
    current_size = 0
    header: list[str] | None = None

    def _open_part():
        nonlocal part_idx, out_f, writer, current_size
        if out_f:
            out_f.close()
        part_idx += 1
        part_path = out_dir / f"{path.stem}_part{part_idx}.csv"
        out_f = part_path.open("w", encoding="utf-8", newline="")
        writer = csv.writer(out_f, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        assert header is not None
        writer.writerow(header)
        # approximate header size
        current_size = sum(len(str(c).encode("utf-8")) for c in header) + len(header) + 1
        parts.append(part_path)
        log.info("Chunk: %s", part_path.name)

    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        _open_part()
        for row in reader:
            row_size = sum(len(str(c).encode("utf-8")) for c in row) + len(row) + 1
            if current_size + row_size > max_bytes and current_size > 0:
                _open_part()
            writer.writerow(row)
            current_size += row_size

    if out_f:
        out_f.close()

    log.info("Gesplitst %s → %d delen in %s", path.name, len(parts), out_dir)
    return parts
