-- Stap 5: publiceer ACTIVE producten op Webshop (published_at leeg + ERP verkoopbaar).
-- Planning NL: dagelijks 04:30, na auto_deactivate (04:00), vóór customs (05:00).
--
-- Vereist op KTM-project: migraties t/m 027 (dispatch_github_workflow_with_inputs met 3 args).
-- Voegt alleen publish-dispatch + 04:30-slot toe; overschrijft geen bestaande dispatch-implementatie.

-- Opruimen: verouderde 2-arg overload uit eerdere 032-poging (conflict met 021/027).
drop function if exists public.dispatch_shopify_publish_sellable_active_workflow();
drop function if exists public.dispatch_github_workflow_with_inputs(text, jsonb);

create or replace function public.dispatch_shopify_publish_sellable_active_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shopify_publish_sellable_active_products.yml'::text,
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

grant execute on function public.dispatch_shopify_publish_sellable_active_workflow() to postgres, service_role;

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

  if h between 7 and 23 and m = 0 then
    perform public.dispatch_price_eta_apply_workflow();
  end if;

  if m = 15 and h in (0, 7, 12, 18) then
    perform public.dispatch_price_eta_policy_workflow();
  end if;
end;
$$;

grant execute on function public.maybe_dispatch_github_workflows_nl_schedule() to postgres, service_role;
