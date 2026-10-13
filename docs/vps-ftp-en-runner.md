# VPS: wekelijkse PIM + nieuwe producten

## Rollen

| Waar | Wat |
|------|-----|
| GitHub Actions `ktm_price_eta_status_sync` | Elke nacht prijs/ETA (kleine CSVs) |
| VPS `motoxshop-vps2` (`149.210.206.111`) | Wekelijkse PIM-zips (XML + PHO) + pipeline via SSH |
| Putoline op dezelfde VPS | Kroon-API (IP-whitelist), 02:15 NL — niet mengen |
| Overige GHA | SEO, publish, barcodes, … |

Geen permanente self-hosted runner: de weekjob SSHt naar de VPS (zelfde patroon als `motox_putoline.yml`).

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
python3 scripts/run_new_products_weekly.py
python3 scripts/run_new_products_weekly.py --skip-pim --skip-rebuild --dry-run-import --import-limit 5
# GHA workflow_dispatch: skip_pim / skip_rebuild / dry_run_import / import_limit
```

Prepare-flags (intern): `--extract-xml-from-zips --extract-media-from-zips --prune-old-xml`.

## GitHub Actions

Workflow: `.github/workflows/ktm_new_products_weekly.yml`

- Schedule: maandag 03:30 UTC
- Secret: `MOTOX_VPS_SSH_KEY` (zelfde als Putoline)
- Concurrency: `shopify-write-lock-ktm`

Handmatig: Actions → **ktm_new_products_weekly** → Run workflow.

## Schijf

- Merk-voor-merk zip ophalen → extract → zip weg
- Alleen nieuwste `CBEXPDN_*.xml` per merkmap
- Staging `downloads/ftp/` legen
- ~93 GB vrij was genoeg bij start; monitor `df -h` na eerste run

## Ready-flag

Na PIM: `output/logs/pim_ready_YYYYMMDD`.
