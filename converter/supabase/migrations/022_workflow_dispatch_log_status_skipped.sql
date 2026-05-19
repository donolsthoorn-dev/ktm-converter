-- 021 gebruikt status skipped/pending; oorspronkelijke check (006) staat alleen queued/failed toe.

alter table public.workflow_dispatch_log
  drop constraint if exists workflow_dispatch_log_status_check;

alter table public.workflow_dispatch_log
  add constraint workflow_dispatch_log_status_check
  check (status in ('queued', 'failed', 'skipped', 'pending'));
