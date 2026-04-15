-- Voeg ERP ArticleStatus + expliciete published-voorkeur toe aan staging.
-- Regels:
--   - proposed_article_status_code = ruwe CSV ArticleStatus (bijv. 20/40/50/60/70/80)
--   - proposed_published = false bij status 80, anders true

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_article_status_code text;

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_published boolean not null default true;
