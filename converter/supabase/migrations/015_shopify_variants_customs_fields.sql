-- Mirror-uitbreiding: inventory item customs velden voor HS-code en land van herkomst.

alter table if exists public.shopify_variants
  add column if not exists inventory_item_id bigint;

alter table if exists public.shopify_variants
  add column if not exists harmonized_system_code text;

alter table if exists public.shopify_variants
  add column if not exists country_code_of_origin text;

create index if not exists shopify_variants_inventory_item_idx
  on public.shopify_variants (inventory_item_id);
