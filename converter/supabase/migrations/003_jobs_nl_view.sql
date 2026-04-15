-- Lees-freundelijke view: *_nl kolommen = wall-clock tijd in Europe/Amsterdam (CET/CEST).
-- Open in Supabase Table Editor: public.jobs_nl  (naast public.jobs).
-- Oorspronkelijke timestamptz-kolommen blijven UTC; *_nl is "timestamp without time zone" (alleen weergave).

create or replace view public.jobs_nl as
select
  id,
  created_at,
  started_at,
  finished_at,
  created_at at time zone 'Europe/Amsterdam' as created_at_nl,
  started_at at time zone 'Europe/Amsterdam' as started_at_nl,
  finished_at at time zone 'Europe/Amsterdam' as finished_at_nl,
  job_type,
  status,
  trigger_source,
  payload,
  log_summary,
  error_message
from public.jobs;

comment on view public.jobs_nl is
  'Zelfde als jobs; extra *_nl kolommen voor lokale tijd Amsterdam (Table Editor / SQL).';

create or replace view public.sync_runs_nl as
select
  id,
  started_at,
  finished_at,
  started_at at time zone 'Europe/Amsterdam' as started_at_nl,
  finished_at at time zone 'Europe/Amsterdam' as finished_at_nl,
  status,
  job_type,
  stats,
  error_message
from public.sync_runs;

comment on view public.sync_runs_nl is
  'Zelfde als sync_runs; *_nl = wall-clock Amsterdam voor lezen in de UI.';
