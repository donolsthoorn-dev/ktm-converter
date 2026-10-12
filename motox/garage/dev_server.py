"""Lokale server voor de garage-preview en `shopify app dev`."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_dotenv, load_project_env  # noqa: E402
from modules.motox_garage.server import GarageApp, config_from_env, serve  # noqa: E402


def main() -> None:
    load_project_env()
    load_dotenv(ROOT / ".env.motox", override=True)
    os.environ.setdefault("MOTOX_GARAGE_PREVIEW", "1")
    config = config_from_env()
    if not config.secret and config.preview:
        config.allow_unsigned = True
        print("Geen SHOPIFY_API_SECRET: app-proxy handtekening wordt niet gecontroleerd.")
    has_app = bool(config.api_key and config.secret and config.shop_domain)
    if not has_app and not config.token:
        config.memory = True
        print("Geen garage-app gekoppeld: ingelogde garage blijft in het geheugen van dit proces.")
    # Shopify CLI zet HOST op de publieke tunnel-URL. Daar kun je niet op binden.
    host = os.environ.get("BIND_HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8787"))
    print(f"Garage-preview: http://127.0.0.1:{port}/preview", flush=True)
    serve(GarageApp(config), host, port)


if __name__ == "__main__":
    main()
