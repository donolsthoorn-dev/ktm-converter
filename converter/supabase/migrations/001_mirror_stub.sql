-- Spiegel-schema: product + variant + prijs + YMM/ETA (stub — pas kolommen aan jullie metafields/export).
-- Voer uit in Supabase: SQL Editor, of via `supabase db push` als je CLI gebruikt.

create table if not exists public.sync_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running' check (status in ('running', 'success', 'failed')),
  job_type text not null default 'overnight_mirror',
  stats jsonb,
  error_message text
);

create index if not exists sync_runs_started_at_idx on public.sync_runs (started_at desc);

create table if not exists public.shopify_products (
  shopify_product_id bigint primary key,
  handle text,
  title text,
  status text,
  published_at timestamptz,
  updated_at_shopify timestamptz,
  raw jsonb,
  synced_at timestamptz not null default now()
);

create table if not exists public.shopify_variants (
  shopify_variant_id bigint primary key,
  shopify_product_id bigint not null references public.shopify_products (shopify_product_id) on delete cascade,
  sku text,
  title text,
  price numeric(18, 4),
  compare_at_price numeric(18, 4),
  updated_at_shopify timestamptz,
  raw jsonb,
  synced_at timestamptz not null default now()
);

create index if not exists shopify_variants_product_idx on public.shopify_variants (shopify_product_id);

-- YMM: pas aan naar jullie model (JSON array, of genormaliseerde child table later).
create table if not exists public.shopify_ymm (
  shopify_product_id bigint primary key references public.shopify_products (shopify_product_id) on delete cascade,
  ymm_json jsonb,
  synced_at timestamptz not null default now()
);

-- ETA (vaak variant-metafield): één rij per variant of per product — hier variant-niveau.
create table if not exists public.shopify_eta (
  shopify_variant_id bigint primary key references public.shopify_variants (shopify_variant_id) on delete cascade,
  eta_date date,
  eta_raw text,
  synced_at timestamptz not null default now()
);

-- Minimale RLS: nog geen auth — later policies op user/role. Nu service role bypassed RLS anyway.
alter table public.sync_runs enable row level security;
alter table public.shopify_products enable row level security;
alter table public.shopify_variants enable row level security;
alter table public.shopify_ymm enable row level security;
alter table public.shopify_eta enable row level security;

-- Geen policies = alleen service role kan lezen/schrijven tot je auth + policies toevoegt.
