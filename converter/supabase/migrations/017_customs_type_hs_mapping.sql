-- Lookup-tabel: Shopify product type -> gekozen HS-code + omschrijving (tariffnumber.com).

create table if not exists public.customs_type_hs_mapping (
  product_type text primary key,
  hs_code text,
  hs_description text,
  hs_description_source text,
  hs_description_url text,
  mapping_source text,
  sku_count integer not null default 0,
  mapped_sku_count integer not null default 0,
  tariff_year integer,
  tariff_lang text,
  updated_at timestamptz not null default now()
);

create index if not exists customs_type_hs_mapping_hs_code_idx
  on public.customs_type_hs_mapping (hs_code);

alter table public.customs_type_hs_mapping enable row level security;
