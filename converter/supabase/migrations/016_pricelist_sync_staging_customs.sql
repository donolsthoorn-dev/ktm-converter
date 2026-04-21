-- Staging-uitbreiding voor customs delta (HS-code + land van herkomst).

alter table if exists public.pricelist_sync_staging
  add column if not exists mirror_inventory_item_id bigint;

alter table if exists public.pricelist_sync_staging
  add column if not exists mirror_hs_code text;

alter table if exists public.pricelist_sync_staging
  add column if not exists mirror_country_of_origin text;

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_hs_code text;

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_country_of_origin text;

alter table if exists public.pricelist_sync_staging
  add column if not exists customs_source text;

alter table if exists public.pricelist_sync_staging
  add column if not exists customs_confidence text;

alter table if exists public.pricelist_sync_staging
  add column if not exists customs_changed boolean not null default false;

alter table if exists public.pricelist_sync_staging
  add column if not exists customs_updated_at timestamptz;

create index if not exists pricelist_sync_staging_customs_idx
  on public.pricelist_sync_staging (customs_changed, customs_updated_at);
