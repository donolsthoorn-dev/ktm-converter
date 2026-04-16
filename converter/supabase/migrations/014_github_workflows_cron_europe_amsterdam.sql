-- Centrale planning op Nederlandse wandtijd (Europe/Amsterdam), omdat pg_cron op UTC draait.
-- Vervangt de losse UTC-cronjobs door één minuut-job die per lokale minuut dispatcht.
--
-- Lokale tijden (Europe/Amsterdam):
--   - ktm_job_worker_nightly (inhoud): dagelijks 03:00
--   - ktm_shopify_auto_deactivate_after_policy (inhoud): dagelijks 04:00
--   - ktm_price_eta_apply_hourly_0700_2300 (inhoud): 07:00 t/m 23:00 elk heel uur (:00, inclusief 23:00)
--   - ktm_price_eta_policy_nightly (inhoud): 00:15, 07:15, 12:15, 18:15
--
-- Oude jobnamen worden uit cron.job verwijderd; monitoring: zoek jobname
--   ktm_github_workflows_nl_minutely

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

  if h between 7 and 23 and m = 0 then
    perform public.dispatch_price_eta_apply_workflow();
  end if;

  if m = 15 and h in (0, 7, 12, 18) then
    perform public.dispatch_price_eta_policy_workflow();
  end if;
end;
$$;

grant execute on function public.maybe_dispatch_github_workflows_nl_schedule() to postgres, service_role;

-- Oude losse UTC-schedules opruimen
do $$
declare
  v_jobid bigint;
begin
  for v_jobid in
    select jobid
    from cron.job
    where jobname in (
      'ktm_job_worker_nightly',
      'ktm_shopify_auto_deactivate_after_policy',
      'ktm_price_eta_apply_hourly_0700_2300',
      'ktm_price_eta_policy_nightly',
      'ktm_github_workflows_nl_minutely'
    )
  loop
    perform cron.unschedule(v_jobid);
  end loop;
end $$;

select cron.schedule(
  'ktm_github_workflows_nl_minutely',
  '* * * * *',
  $$select public.maybe_dispatch_github_workflows_nl_schedule();$$
);
