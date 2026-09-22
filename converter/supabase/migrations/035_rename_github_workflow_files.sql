-- Hernoem GitHub-workflowbestanden naar ktm_ (alleen ktm-shop.nl) en shops_ (beide shops).
-- Zie docs/workflow-namen.md.
--
-- Dispatch stuurt alleen de nieuwe bestandsnaam. De write-lock en de generieke
-- lock-check herkennen ook de oude namen, zodat een run die vóór deze migratie
-- startte de lock nog vasthoudt tot hij klaar is.
-- Oude migraties blijven historisch; deze vervangt de live functies.

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
      'motox_synkro_clone_missing.yml'
    )
      and w.run_state = 'running'
      and w.run_started_at > now() - interval '12 hours'
  );
$$;

create or replace function public.dispatch_github_workflow(
  p_workflow_file text,
  p_mode text default null
)
returns bigint
language plpgsql
security definer
as $$
declare
  v_inputs jsonb := '{}'::jsonb;
  v_requires_lock boolean := p_workflow_file in (
    'ktm_price_eta_status_sync.yml',
    'price_eta_status_sync.yml',
    'ktm_customs_missing_fill.yml',
    'customs_missing_fill.yml',
    'ktm_shopify_auto_deactivate_invalid_products.yml',
    'shopify_auto_deactivate_invalid_products.yml'
  );
begin
  if p_mode is not null and trim(p_mode) <> '' then
    v_inputs := jsonb_build_object('mode', trim(p_mode));
  end if;
  return public.dispatch_github_workflow_with_inputs(
    p_workflow_file,
    v_inputs,
    v_requires_lock
  );
end;
$$;

create or replace function public.dispatch_job_worker_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs('ktm_job_worker.yml', '{}'::jsonb, false);
$$;

create or replace function public.dispatch_price_eta_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'ktm_price_eta_status_sync.yml',
    jsonb_build_object('mode', 'apply', 'apply_scope', 'price_eta'),
    true
  );
$$;

create or replace function public.dispatch_price_eta_policy_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'ktm_price_eta_status_sync.yml',
    jsonb_build_object('mode', 'apply', 'apply_scope', 'policy'),
    true
  );
$$;

create or replace function public.dispatch_shopify_auto_deactivate_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'ktm_shopify_auto_deactivate_invalid_products.yml',
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

create or replace function public.dispatch_customs_missing_fill_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'ktm_customs_missing_fill.yml',
    '{}'::jsonb,
    true
  );
$$;

create or replace function public.dispatch_shopify_publish_sellable_active_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'ktm_shopify_publish_sellable_active_products.yml'::text,
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

create or replace function public.dispatch_shopify_gtin_fill_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'ktm_shopify_gtin_fill.yml'::text,
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

create or replace function public.dispatch_motox_synkro_clone_missing_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shops_synkro_clone_missing.yml'::text,
    jsonb_build_object(
      'apply', 'true',
      'max_products', '40',
      'scope', 'telling'
    ),
    true
  );
$$;

grant execute on function public.dispatch_job_worker_workflow() to postgres, service_role;
grant execute on function public.dispatch_price_eta_apply_workflow() to postgres, service_role;
grant execute on function public.dispatch_price_eta_policy_workflow() to postgres, service_role;
grant execute on function public.dispatch_shopify_auto_deactivate_apply_workflow() to postgres, service_role;
grant execute on function public.dispatch_customs_missing_fill_workflow() to postgres, service_role;
grant execute on function public.dispatch_shopify_publish_sellable_active_workflow() to postgres, service_role;
grant execute on function public.dispatch_shopify_gtin_fill_workflow() to postgres, service_role;
grant execute on function public.dispatch_motox_synkro_clone_missing_workflow() to postgres, service_role;
