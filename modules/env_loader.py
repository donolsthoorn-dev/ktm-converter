"""Load project root `.env` into os.environ (does not override existing vars)."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path | None = None, *, override: bool = False) -> None:
    """
    Minimal KEY=value parser (no python-dotenv dependency).
    Lines: empty, # comments, KEY=value (optional quotes stripped).

    If override is False (default), a key is only set when it is not already present
    in os.environ (shell exports win).

    If override is True, keys from this file always replace existing values.
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
                if override or key not in os.environ:
                    os.environ[key] = val
    except OSError:
        return


def load_project_env() -> None:
    """
    Laadt omgevingsvariabelen voor scripts vanaf de repo-root:

    1. ``.env`` (vult alleen ontbrekende keys t.o.v. de shell)
    2. ``converter/.env`` (idem)
    3. ``converter/.env.local`` (overschrijft — typisch Next/Vercel secrets lokaal)

    Daarna: als ``SUPABASE_URL`` leeg is maar ``NEXT_PUBLIC_SUPABASE_URL`` wel gezet
    is (zoals in de converter-app), wordt ``SUPABASE_URL`` daaruit gezet zodat
    Python-scripts dezelfde Supabase-host kunnen gebruiken.
    """
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)
    load_dotenv(root / "converter" / ".env", override=False)
    load_dotenv(root / "converter" / ".env.local", override=True)
    if not (os.environ.get("SUPABASE_URL") or "").strip():
        n = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or "").strip()
        if n:
            os.environ["SUPABASE_URL"] = n
