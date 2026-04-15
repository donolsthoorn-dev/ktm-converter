-- Mirror-uitbreiding: bewaar variant inventory policy (deny/continue).
-- Nodig voor staging van "Sell when out of stock".

alter table if exists public.shopify_variants
  add column if not exists inventory_policy text;
