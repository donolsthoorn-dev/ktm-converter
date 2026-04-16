-- Voeg producttype toe aan de Shopify product-spiegel.
alter table if exists public.shopify_products
  add column if not exists type text;

comment on column public.shopify_products.type is
  'Shopify productType uit Admin GraphQL';
