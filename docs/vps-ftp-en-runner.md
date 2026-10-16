# VPS: wekelijkse PIM + nieuwe producten

## Rollen

| Waar | Wat |
|------|-----|
| GitHub Actions `ktm_price_eta_status_sync` | Prijs/ETA (kleine CSVs; dispatcher/uur) |
| VPS `motoxshop-vps2` (`149.210.206.111`) | PIM-zips (XML + PHO) + pipeline via SSH |
| Putoline op dezelfde VPS | Kroon-API (IP-whitelist), 02:15 NL — niet mengen |
| Overige GHA | SEO, publish, barcodes, … |

Geen permanente self-hosted runner: `ktm_new_products_daily` SSHt naar de VPS (zelfde patroon als `motox_putoline.yml`).  
Schedule: **dagelijks 04:00 / 05:30 / 08:00 UTC** (noop als PIM-zips ongewijzigd; backup-crons skippen via OK-marker als vandaag al een substantiële run slaagde). Watchdog `ktm_new_products_watchdog` triggert de daily als er tot 05–11 UTC nog geen succes was. Eigen GHA-concurrency `ktm-new-products-daily` (niet de gedeelde `shopify-write-lock-ktm`, anders cancelt uurlijkse prijs/ETA pending runs). Soft lock via Supabase + wait tot andere KTM-schrijfjobs klaar zijn.

## VPS-setup

Pad: `/home/motoxshop/ktm_project` (code via rsync vanuit GHA of laptop; VPS heeft geen GitHub-deploy-key).

```bash
# lokaal sync (voorbeeld)
rsync -az --exclude input/ --exclude output/ --exclude .venv/ --exclude .git/ \
  ./ motoxshop@149.210.206.111:/home/motoxshop/ktm_project/

ssh motoxshop@149.210.206.111
cd /home/motoxshop/ktm_project
python3 -m venv .venv
.venv/bin/pip install 'requests>=2.28,<3' 'paramiko>=3,<4' 'lxml>=4.9'
# .env met FTP + Shopify + Supabase (chmod 600)
```

Hardware-mitigatie zonder RAM-upgrade: **4 GB swapfile** (`/swapfile`) — al gezet.

## Commando’s

Alleen PIM (FTP → extract → cleanup):

```bash
cd /home/motoxshop/ktm_project
python3 scripts/vps_pim_refresh_weekly.py
```

Volledige weekpipeline:

```bash
python3 scripts/run_new_products_daily.py
python3 scripts/run_new_products_daily.py --skip-pim --skip-rebuild --dry-run-import --import-limit 5
# GHA: skip_pim / skip_rebuild / force_pim / dry_run_import / import_limit
# PIM-zip FTP fingerprint (size/mtime): ongewijzigd → pipeline stopt (~seconden).
# Prijs-CSV-wijzigingen tellen niet mee (nachtelijke sync).
# Na echte PIM-refresh: missing images + YMM/complementary (ook bij created=0).
# Digest toont “Extra foto’s toegevoegd N” als er images zijn bijgezet.
```

Prepare-flags (intern): `--extract-xml-from-zips --extract-media-from-zips --prune-old-xml`.

## GitHub Actions

Workflow: `.github/workflows/ktm_new_products_daily.yml`

- Schedule: 04:00 / 05:30 / 08:00 UTC (+ watchdog 05–11 UTC)
- Secret: `MOTOX_VPS_SSH_KEY` (zelfde als Putoline)
- Concurrency: `ktm-new-products-daily` + Supabase soft lock (migratie 039)
- Na YMM-push: `ymm_ensure_updatable.py` (pending diffs + backfill lege velden)

Handmatig: Actions → **ktm_new_products_daily** → Run workflow.

## Schijf

- Merk-voor-merk zip ophalen → extract → zip weg
- Alleen nieuwste `CBEXPDN_*.xml` per merkmap
- Staging `downloads/ftp/` legen
- ~93 GB vrij was genoeg bij start; monitor `df -h` na eerste run

## Ready-flag

Na PIM: `output/logs/pim_ready_YYYYMMDD`.
