-- Plan: laat policy-apply 's nachts draaien en status80/voorraad-publicatiecheck daarna.
-- Nieuwe planning (UTC):
--   - price_eta_status_sync.yml (apply_scope=policy): dagelijks 01:00
--   - shopify_auto_deactivate_invalid_products.yml (apply=true): dagelijks 05:30

create or replace function public.dispatch_price_eta_policy_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'price_eta_status_sync.yml',
    jsonb_build_object('mode', 'apply', 'apply_scope', 'policy')
  );
$$;

grant execute on function public.dispatch_price_eta_policy_workflow() to postgres, service_role;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_price_eta_policy_nightly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

do $$
declare
  v_jobid bigint;
begin
  -- oude deactivate planning vervangen door run na policy-window
  select jobid into v_jobid from cron.job where jobname = 'ktm_shopify_auto_deactivate_0500';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_shopify_auto_deactivate_after_policy';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

select cron.schedule(
  'ktm_price_eta_policy_nightly',
  '0 1 * * *',
  $$select public.dispatch_price_eta_policy_workflow();$$
);

select cron.schedule(
  'ktm_shopify_auto_deactivate_after_policy',
  '30 5 * * *',
  $$select public.dispatch_shopify_auto_deactivate_apply_workflow();$$
);
