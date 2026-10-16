-- ktm_new_products_daily gebruikt geen GHA shopify-write-lock-ktm meer
-- (die wachtrij cancelt pending runs). In plaats daarvan claimt de workflow
-- een soft lock via workflow_dispatch_log, zodat Supabase-dispatches
-- (prijs/ETA, Synkro, …) skippen zolang nieuwe-producten draait.

create or replace function public.shopify_write_lock_is_busy()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.workflow_dispatch_log w
    where w.workflow_file in (
      'ktm_price_eta_status_sync.yml',
      'price_eta_status_sync.yml',
      'ktm_customs_missing_fill.yml',
      'customs_missing_fill.yml',
      'ktm_shopify_auto_deactivate_invalid_products.yml',
      'shopify_auto_deactivate_invalid_products.yml',
      'ktm_shopify_publish_sellable_active_products.yml',
      'shopify_publish_sellable_active_products.yml',
      'ktm_shopify_gtin_fill.yml',
      'shopify_gtin_fill.yml',
      'shops_synkro_clone_missing.yml',
      'motox_synkro_clone_missing.yml',
      'motox_putoline.yml',
      'ktm_new_products_daily.yml'
    )
      and w.run_state = 'running'
      and w.run_started_at > now() - interval '12 hours'
  );
$$;
