"""Vercel-host voor de Motox-garage. De winkel praat hiermee, niet met een laptop."""

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


def _fitment_path() -> Path | None:
    for candidate in (HERE / "data" / "fitment.bin", ROOT / "motox" / "garage" / "data" / "fitment.bin"):
        if candidate.is_file():
            return candidate
    return None


def garage() -> GarageApp:
    global _garage
    if _garage is not None:
        return _garage
    os.environ.setdefault("SHOPIFY_MOTOX_SHOP_DOMAIN", "ktm-shop-nederland.myshopify.com")
    os.environ.setdefault("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nederland.myshopify.com")
    config = config_from_env()
    if not config.shop_domain:
        config.shop_domain = "ktm-shop-nederland.myshopify.com"
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
  <title>Motox garage</title>
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
    <p>Motox garage is het motorfilter van motocrossshop.nl. Een bezoeker kiest zijn motor. Die blijft staan in de rode balk bovenaan de winkel. Op een collectiepagina kan hij daarna alleen de onderdelen voor die motor laten zien. Op de productpagina staat of het artikel op de gekozen motor past. Het artikel blijft gewoon te koop.</p>
    <p>Dit scherm in de Shopify-admin is alleen de uitleg. Het filter zelf zit op de winkel, niet hier.</p>

    <h2>Hoe het werkt</h2>
    <p>De gekozen motor wordt bewaard in een cookie op het apparaat van de bezoeker. Komt hij later terug, dan staat dezelfde motor er nog. Is hij ingelogd, dan probeert de app die keuze ook op zijn klantaccount te zetten.</p>
    <p>Het vinkje “Toon alleen onderdelen voor …” geldt voor de hele sessie en staat standaard aan. Het staat niet in het webadres. Zet een bezoeker het uit, dan blijft de volledige collectie staan tot hij het weer aanzet of de sessie voorbij is.</p>
    <p>Staat het vinkje aan, dan vraagt de winkel aan deze app welke artikelen bij die motor horen. De app leest de collectie, vergelijkt die met de fitment-index en stuurt het gefilterde overzicht terug. De pagina vervangt het raster daarna door alleen die artikelen. Komt het gewone Shopify-raster later alsnog terug, dan zet het filter zichzelf opnieuw aan, zonder dat de bezoeker de pagina hoeft te verversen.</p>

    <h2>Wat wel en niet verdwijnt</h2>
    <ul>
      <li>De bron is het metafield <strong>global.fits_on</strong> van Motox. Daar staat op welke motoren een artikel past.</li>
      <li>Een artikel mét motor-fitment dat niet op de gekozen motor past, verdwijnt uit het overzicht zolang het vinkje aan staat.</li>
      <li>Een artikel zonder fitment, zoals een helm, olie of kleding, blijft zichtbaar. Dat hoort universeel te zijn.</li>
      <li>Een kassakopie van KTM, Husqvarna, GasGas of WP met de tag <strong>synkro-clone-ktm-shop-nederland</strong> en een lege fits_on valt wél weg. Die kopie staat in Motox voor de kassa, niet als webshop-fitment.</li>
      <li>E-bikes horen niet in dit motorfilter.</li>
    </ul>

    <h2>Waar het draait</h2>
    <p>Op de winkel draait een app-embed in het thema. Die toont de rode balk, het vinkje en de past-op-tekst. De winkel roept het filter aan via <strong>/apps/garage</strong>. Shopify stuurt dat verzoek door naar <strong>motox-garage.vercel.app</strong>.</p>
    <p>De filterlogica en de fitment-index draaien op Vercel, in het project motox-garage. De index is een compacte kopie van de fits_on-gegevens, ongeveer 15 MB, en reist mee met die app. De winkel hangt daar niet meer aan een laptop of een tunnel.</p>
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
        "Motox garage is gekoppeld aan de winkel. Je kunt dit venster sluiten.",
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
    saved = requests.post(
        f"https://api.vercel.com/v10/projects/{project}/env{team_q}&upsert=true"
        if team
        else f"https://api.vercel.com/v10/projects/{project}/env?upsert=true",
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
    _drop_install_secrets(requests, headers, project, team_q)
    deployment = (os.environ.get("VERCEL_DEPLOYMENT_ID") or "").strip()
    if not deployment:
        return
    joiner = "&" if team_q else "?"
    redeploy = requests.post(
        f"https://api.vercel.com/v13/deployments{team_q}{joiner}forceNew=1",
        headers=headers,
        json={
            "name": "motox-garage",
            "deploymentId": deployment,
            "target": "production",
            "meta": {"action": "redeploy"},
        },
        timeout=(15, 60),
    )
    if redeploy.status_code >= 400:
        raise RuntimeError("herstart mislukt")


def _drop_install_secrets(requests, headers, project: str, team_q: str) -> None:
    listed = requests.get(
        f"https://api.vercel.com/v9/projects/{project}/env{team_q}",
        headers=headers,
        timeout=(15, 60),
    )
    if listed.status_code >= 400:
        return
    for item in (listed.json() or {}).get("envs") or []:
        if item.get("key") != "VERCEL_TOKEN":
            continue
        env_id = item.get("id") or ""
        if not env_id:
            continue
        requests.delete(
            f"https://api.vercel.com/v9/projects/{project}/env/{env_id}{team_q}",
            headers=headers,
            timeout=(15, 60),
        )


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
