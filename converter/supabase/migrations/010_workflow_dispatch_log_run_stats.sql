-- Enrich workflow_dispatch_log with runtime metadata and summary stats.

alter table if exists public.workflow_dispatch_log
  add column if not exists github_run_id bigint;

alter table if exists public.workflow_dispatch_log
  add column if not exists run_state text;

alter table if exists public.workflow_dispatch_log
  add column if not exists run_started_at timestamptz;

alter table if exists public.workflow_dispatch_log
  add column if not exists run_finished_at timestamptz;

alter table if exists public.workflow_dispatch_log
  add column if not exists run_summary text;

alter table if exists public.workflow_dispatch_log
  add column if not exists run_stats jsonb;

create index if not exists workflow_dispatch_log_workflow_created_idx
  on public.workflow_dispatch_log (workflow_file, created_at desc);
