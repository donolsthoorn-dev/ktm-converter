-- Dagelijkse Actions-digest om 10:00 Europe/Amsterdam.
-- Workflow: daily_actions_digest.yml (Resend-mail met gelukte updates).

create or replace function public.dispatch_daily_actions_digest_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'daily_actions_digest.yml'::text,
    jsonb_build_object('send', 'true'),
    false
  );
$$;

grant execute on function public.dispatch_daily_actions_digest_workflow() to postgres, service_role;

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

  if h = 10 and m = 0 then
    perform public.dispatch_daily_actions_digest_workflow();
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
