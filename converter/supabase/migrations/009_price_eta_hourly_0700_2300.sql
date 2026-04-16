-- Update price/ETA apply schedule:
-- run hourly between 07:00 and 23:00 UTC.

do $$
declare
  v_jobid bigint;
begin
  -- oude 3-uurs jobnaam opruimen
  select jobid into v_jobid from cron.job where jobname = 'ktm_price_eta_apply_3hourly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

do $$
declare
  v_jobid bigint;
begin
  -- idem voor eventuele bestaande uurlijkse variant
  select jobid into v_jobid from cron.job where jobname = 'ktm_price_eta_apply_hourly_0700_2300';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

select cron.schedule(
  'ktm_price_eta_apply_hourly_0700_2300',
  '0 7-23 * * *',
  $$select public.dispatch_price_eta_apply_workflow();$$
);
