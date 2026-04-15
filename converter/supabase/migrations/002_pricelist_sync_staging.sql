-- Staging: verschillen tussen KTM prijs-CSV (FTP) en Supabase-spiegel (shopify_*), ter review vóór Shopify API.
-- Vul via: python scripts/build_pricelist_supabase_staging.py
-- Pas toe op Shopify: aparte stap (nog te bouwen) op basis van review_status = approved.

create table if not exists public.pricelist_sync_staging (
  id uuid primary key default gen_random_uuid(),
  batch_id uuid not null,
  created_at timestamptz not null default now(),

  sku text not null,
  shopify_variant_id bigint not null,
  shopify_product_id bigint,

  mirror_price numeric(18, 4),
  mirror_eta_date date,
  mirror_product_status text,

  proposed_price numeric(18, 4),
  proposed_eta_date date,
  proposed_product_status text,

  price_changed boolean not null default false,
  eta_changed boolean not null default false,
  status_changed boolean not null default false,

  review_status text not null default 'pending'
    check (review_status in ('pending', 'approved', 'rejected', 'applied')),
  notes text,

  unique (batch_id, shopify_variant_id)
);

create index if not exists pricelist_sync_staging_batch_idx
  on public.pricelist_sync_staging (batch_id desc);
create index if not exists pricelist_sync_staging_review_idx
  on public.pricelist_sync_staging (review_status);
create index if not exists pricelist_sync_staging_sku_idx
  on public.pricelist_sync_staging (sku);

alter table public.pricelist_sync_staging enable row level security;
