-- Voeg ERP ArticleStatus + variant sell-when-out-of-stock velden toe aan staging.
-- Regels:
--   - proposed_article_status_code = ruwe CSV ArticleStatus (bijv. 20/40/50/60/70/80)
--   - proposed_inventory_policy = bij status 80 -> DENY (sell_when_out_of_stock OFF)
--   - proposed_sell_when_out_of_stock = false bij status 80, anders null (geen wijziging)
--   - mirror_inventory_policy / inventory_policy_changed voor vergelijking

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_article_status_code text;

alter table if exists public.pricelist_sync_staging
  add column if not exists mirror_inventory_policy text;

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_inventory_policy text;

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_sell_when_out_of_stock boolean;

alter table if exists public.pricelist_sync_staging
  add column if not exists inventory_policy_changed boolean not null default false;

alter table if exists public.pricelist_sync_staging
  drop column if exists proposed_published;
