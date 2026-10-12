"""Vercel-host voor de KTM-garage. De winkel praat hiermee, niet met een laptop."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from flask import Flask, Response, request

HERE = Path(__file__).resolve().parent
ROOT = HERE if (HERE / "modules").is_dir() else HERE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.motox_garage.fitment_compact import CompactFitment  # noqa: E402
from modules.motox_garage.oauth import exchange_offline_token, verify_oauth_hmac  # noqa: E402
from modules.motox_garage.server import GarageApp, config_from_env  # noqa: E402

app = Flask(__name__)
_garage: GarageApp | None = None
_DEFAULT_SHOP = "ktm-shop-nl.myshopify.com"


def _fitment_path() -> Path | None:
    for candidate in (HERE / "data" / "fitment.bin", ROOT / "ktm" / "garage" / "data" / "fitment.bin"):
        if candidate.is_file():
            return candidate
    return None


def garage() -> GarageApp:
    global _garage
    if _garage is not None:
        return _garage
    os.environ.setdefault("SHOPIFY_MOTOX_SHOP_DOMAIN", _DEFAULT_SHOP)
    os.environ.setdefault("SHOPIFY_SHOP_DOMAIN", _DEFAULT_SHOP)
    config = config_from_env()
    if not config.shop_domain:
        config.shop_domain = _DEFAULT_SHOP
    path = _fitment_path()
    index = CompactFitment.load(path) if path is not None else None
    created = GarageApp(config, index=index)
    if index is None:
        created._index = None
        created._index_loaded = True
    _garage = created
    return created


_HOME = """<!doctype html>
<html lang="nl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KTM garage</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
  <style>
    html, body { margin: 0; padding: 0; background: transparent; }
    body {
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      font-size: 14px;
      line-height: 1.6;
      color: #202223;
      -webkit-font-smoothing: antialiased;
    }
    main {
      max-width: 40rem;
      padding: 0.25rem 0 2.5rem 1.75rem;
    }
    h2 {
      margin: 1.6rem 0 0.35rem;
      font-size: 15px;
      font-weight: 600;
      letter-spacing: -0.011em;
      line-height: 1.3;
    }
    h2:first-child { margin-top: 0; }
    p { margin: 0 0 0.7rem; }
    ul { margin: 0 0 0.7rem; padding-left: 1.15rem; }
    li { margin: 0 0 0.35rem; }
    li:last-child { margin-bottom: 0; }
  </style>
</head>
<body>
  <main>
    <h2>Wat deze app doet</h2>
    <p>KTM garage is het motorfilter van ktm-shop.nl. Een bezoeker kiest zijn motor. Die blijft staan in de balk bovenaan de winkel. Op een collectiepagina kan hij daarna alleen de onderdelen voor die motor laten zien. Op de productpagina staat of het artikel op de gekozen motor past.</p>
    <p>Dit scherm in de Shopify-admin is alleen de uitleg. Het filter zelf zit op de winkel, niet hier.</p>

    <h2>Hoe het werkt</h2>
    <p>De gekozen motor wordt bewaard in een cookie. Is de bezoeker ingelogd, dan probeert de app die keuze ook op zijn klantaccount te zetten.</p>
    <p>Het vinkje “Toon alleen onderdelen voor …” geldt voor de sessie. Staat het aan, dan vraagt de winkel aan deze app welke artikelen bij die motor horen.</p>

    <h2>Bron</h2>
    <ul>
      <li>Fitment komt uit <strong>global.fits_on</strong> en <strong>global.ymm_summary</strong> op ktm-shop.nl.</li>
      <li>Merken in dit filter: KTM, Husqvarna en GASGAS.</li>
      <li>De winkel roept het filter aan via <strong>/apps/garage</strong>. Shopify stuurt dat door naar <strong>ktmshop-garage.vercel.app</strong>.</li>
    </ul>
  </main>
</body>
</html>
"""


@app.route("/", methods=["GET", "HEAD"])
def home() -> Response:
    if request.method == "HEAD":
        return Response("", status=200)
    return Response(_HOME, status=200, mimetype="text/html")


@app.route("/api/auth", methods=["GET"])
def install_auth() -> Response:
    """Eenmalige installatie: ruilt de OAuth-code voor de offline token."""
    query = request.query_string.decode("utf-8", errors="replace")
    config = garage().config
    shop = (request.args.get("shop") or "").strip().lower()
    expected = (config.shop_domain or "").strip().lower()
    code = (request.args.get("code") or "").strip()
    if not expected or shop != expected or not code:
        return Response("Winkel of code klopt niet.", status=400, mimetype="text/plain")
    if not verify_oauth_hmac(query, config.secret):
        return Response("Handtekening klopt niet.", status=401, mimetype="text/plain")
    try:
        token = exchange_offline_token(shop, config.api_key, config.secret, code)
        _store_garage_token(token)
    except Exception:
        return Response("Koppelen is niet gelukt.", status=502, mimetype="text/plain")
    return Response(
        "KTM garage is gekoppeld aan de winkel. Je kunt dit venster sluiten.",
        status=200,
        mimetype="text/plain; charset=utf-8",
    )


def _store_garage_token(token: str) -> None:
    vercel_token = (os.environ.get("VERCEL_TOKEN") or "").strip()
    project = (os.environ.get("VERCEL_PROJECT_ID") or "").strip()
    team = (os.environ.get("VERCEL_TEAM_ID") or "").strip()
    if not vercel_token or not project:
        raise RuntimeError("vercel ontbreekt")
    import requests

    headers = {"Authorization": f"Bearer {vercel_token}", "Content-Type": "application/json"}
    team_q = f"?teamId={team}" if team else ""
    url = (
        f"https://api.vercel.com/v10/projects/{project}/env{team_q}&upsert=true"
        if team
        else f"https://api.vercel.com/v10/projects/{project}/env?upsert=true"
    )
    saved = requests.post(
        url,
        headers=headers,
        json={
            "key": "SHOPIFY_MOTOX_GARAGE_TOKEN",
            "value": token,
            "type": "sensitive",
            "target": ["production", "preview", "development"],
        },
        timeout=(15, 60),
    )
    if saved.status_code >= 400:
        saved = requests.post(
            url,
            headers=headers,
            json={
                "key": "SHOPIFY_MOTOX_GARAGE_TOKEN",
                "value": token,
                "type": "encrypted",
                "target": ["production", "preview", "development"],
            },
            timeout=(15, 60),
        )
    if saved.status_code >= 400:
        raise RuntimeError("token opslaan mislukt")


@app.route("/<path:path>", methods=["GET", "POST", "PUT", "HEAD"])
def dispatch(path: str) -> Response:
    method = "GET" if request.method == "HEAD" else request.method
    status, payload = garage().handle(
        method,
        "/" + path,
        request.query_string.decode("utf-8", errors="replace"),
        request.get_data(cache=True),
    )
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return Response(data, status=status, mimetype="application/json")
