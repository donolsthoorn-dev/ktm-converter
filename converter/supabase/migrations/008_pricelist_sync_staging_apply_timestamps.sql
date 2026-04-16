-- Audit-timestamps per staging-rij voor succesvolle Shopify apply-mutaties.
-- Wordt gezet door scripts/shopify_apply_from_pricelist_staging.py na succesvolle writes.

alter table if exists public.pricelist_sync_staging
  add column if not exists price_updated_at timestamptz;

alter table if exists public.pricelist_sync_staging
  add column if not exists eta_updated_at timestamptz;

alter table if exists public.pricelist_sync_staging
  add column if not exists policy_updated_at timestamptz;
