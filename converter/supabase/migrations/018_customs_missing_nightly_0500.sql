-- Nachtelijke customs-aanvulrun op Nederlandse tijd.
-- Doel: om 05:00 Europe/Amsterdam workflow dispatchen die varianten met missende
-- HS/COO aanvult op basis van `customs_type_hs_mapping` + COO default.

create or replace function public.dispatch_customs_missing_fill_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow('customs_missing_fill.yml', null);
$$;

grant execute on function public.dispatch_customs_missing_fill_workflow() to postgres, service_role;

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

  if h = 5 and m = 0 then
    perform public.dispatch_customs_missing_fill_workflow();
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
