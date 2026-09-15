-- GTIN/barcode: nachtelijke inhaal-fill + staging-kolom voor meeliften op price/ETA.
--
-- 05:30 Europe/Amsterdam: shopify_gtin_fill.yml (fill-if-empty over de catalogus).
-- Eerste nacht is lang en houdt shopify-write-lock bezet; daarna bijna leeg.
-- Doorlopend: price_eta_status_sync zet proposed_barcode op bestaande delta-rijen.

alter table if exists public.pricelist_sync_staging
  add column if not exists proposed_barcode text;

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
      'price_eta_status_sync.yml',
      'customs_missing_fill.yml',
      'shopify_auto_deactivate_invalid_products.yml',
      'shopify_publish_sellable_active_products.yml',
      'shopify_gtin_fill.yml'
    )
      and w.run_state = 'running'
      and w.run_started_at > now() - interval '12 hours'
  );
$$;

create or replace function public.dispatch_shopify_gtin_fill_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shopify_gtin_fill.yml'::text,
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

grant execute on function public.dispatch_shopify_gtin_fill_workflow() to postgres, service_role;

create or replace function public.maybe_dispatch_github_workflows_nl_schedule()
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  nl timestamp without time zone := (now() at time zone 'Europe/Amsterdam');
  h int := extract(hour from nl)::int;
  m int := extract(minute from nl)::int;
begin
  if h = 3 and m = 0 then
    perform public.dispatch_job_worker_workflow();
  end if;

  if h = 4 and m = 0 then
    perform public.dispatch_shopify_auto_deactivate_apply_workflow();
  end if;

  if h = 4 and m = 30 then
    perform public.dispatch_shopify_publish_sellable_active_workflow();
  end if;

  if h = 5 and m = 0 then
    perform public.dispatch_customs_missing_fill_workflow();
  end if;

  if h = 5 and m = 30 then
    perform public.dispatch_shopify_gtin_fill_workflow();
  end if;

  if h between 7 and 23 and m = 0 then
    perform public.dispatch_price_eta_apply_workflow();
  end if;

  if m = 15 and h in (0, 7, 12, 18) then
    perform public.dispatch_price_eta_policy_workflow();
  end if;
end;
$$;

grant execute on function public.maybe_dispatch_github_workflows_nl_schedule() to postgres, service_role;
