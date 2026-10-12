-- Putoline-delta op Motox, elke nacht 02:15 Europe/Amsterdam.
-- Workflow: motox_putoline.yml. De runner logt in op de VPS en haalt daar
-- het Kroon-assortiment op (IP-whitelist). Zie docs/nachtelijke-jobs.md.

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
      'motox_putoline.yml'
    )
      and w.run_state = 'running'
      and w.run_started_at > now() - interval '12 hours'
  );
$$;

create or replace function public.dispatch_motox_putoline_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'motox_putoline.yml',
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

grant execute on function public.dispatch_motox_putoline_workflow() to postgres, service_role;

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
  if h = 2 and m = 15 then
    perform public.dispatch_motox_putoline_workflow();
  end if;

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

  if m = 30 and h between 7 and 22 then
    perform public.dispatch_motox_synkro_clone_missing_workflow();
  end if;
end;
$$;

grant execute on function public.maybe_dispatch_github_workflows_nl_schedule() to postgres, service_role;
