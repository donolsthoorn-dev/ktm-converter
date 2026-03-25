"""Load project root `.env` into os.environ (does not override existing vars)."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path | None = None) -> None:
    """
    Minimal KEY=value parser (no python-dotenv dependency).
    Lines: empty, # comments, KEY=value (optional quotes stripped).
    """
    if path is None:
        root = Path(__file__).resolve().parents[1]
        path = root / ".env"
    path = Path(path)
    if not path.is_file():
        return
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                if not key:
                    continue
                val = val.strip()
                if (val.startswith('"') and val.endswith('"')) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                if key not in os.environ:
                    os.environ[key] = val
    except OSError:
        return
