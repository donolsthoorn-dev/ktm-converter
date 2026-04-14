-- Job-queue + logging voor handmatige en geplande runs (worker zet status + logs).

create table if not exists public.jobs (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz,
  job_type text not null,
  status text not null default 'queued'
    check (status in ('queued', 'running', 'success', 'failed', 'cancelled')),
  trigger_source text not null default 'manual'
    check (trigger_source in ('manual', 'schedule', 'api')),
  payload jsonb not null default '{}'::jsonb,
  log_summary text,
  error_message text
);

create index if not exists jobs_created_at_idx on public.jobs (created_at desc);
create index if not exists jobs_status_created_idx on public.jobs (status, created_at asc);

comment on table public.jobs is 'ETL/sync jobs; ingevoegd via API of worker, uitgelezen in converter UI.';

alter table public.jobs enable row level security;
