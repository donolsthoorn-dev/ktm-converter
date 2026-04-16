-- Policy apply vier maal per dag (UTC), minuut 15 om overlap met hourly price_eta (minuut 0, uren 7–23) te vermijden.
-- Tijden: 00:15, 06:15, 12:15, 18:15 UTC.
-- Vereist: public.dispatch_price_eta_policy_workflow() (migratie 011).

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_price_eta_policy_nightly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

select cron.schedule(
  'ktm_price_eta_policy_nightly',
  '15 0,6,12,18 * * *',
  $$select public.dispatch_price_eta_policy_workflow();$$
);
