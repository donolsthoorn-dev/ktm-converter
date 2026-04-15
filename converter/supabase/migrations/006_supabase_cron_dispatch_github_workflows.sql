-- Supabase-driven scheduling for GitHub workflows (more reliable than GitHub schedule).
--
-- Schedules:
--   - Job worker: hourly at :00 UTC
--   - price_eta_status_sync (apply): hourly at :30 UTC
--
-- Prerequisites (run once in Supabase SQL editor, project admin role):
--   select vault.create_secret('<github_pat_with_actions_write>', 'github_actions_pat', 'PAT for workflow dispatch');
--   -- optional overrides:
--   -- select vault.create_secret('main', 'github_repo_ref', 'Target ref');
--   -- select vault.create_secret('donolsthoorn-dev', 'github_repo_owner', 'Owner');
--   -- select vault.create_secret('ktm-converter', 'github_repo_name', 'Repo');
--
-- PAT scopes:
--   - classic token: repo + workflow
--   - fine-grained token: Actions read/write + Contents read/write on this repo

create extension if not exists pg_net;
create extension if not exists pg_cron;
create extension if not exists supabase_vault;

create table if not exists public.workflow_dispatch_log (
  id bigint generated always as identity primary key,
  created_at timestamptz not null default now(),
  workflow_file text not null,
  mode text,
  request_id bigint,
  status text not null check (status in ('queued', 'failed')),
  error_message text
);

create index if not exists workflow_dispatch_log_created_idx
  on public.workflow_dispatch_log (created_at desc);

alter table public.workflow_dispatch_log enable row level security;

create or replace function public.dispatch_github_workflow(
  p_workflow_file text,
  p_mode text default null
)
returns bigint
language plpgsql
security definer
set search_path = public, vault
as $$
declare
  v_owner text;
  v_repo text;
  v_ref text;
  v_pat text;
  v_url text;
  v_request_id bigint;
  v_inputs jsonb;
begin
  select decrypted_secret into v_pat
  from vault.decrypted_secrets
  where name = 'github_actions_pat'
  limit 1;

  if v_pat is null or length(trim(v_pat)) = 0 then
    raise exception 'Vault secret github_actions_pat ontbreekt';
  end if;

  select coalesce(nullif(trim(decrypted_secret), ''), 'donolsthoorn-dev') into v_owner
  from vault.decrypted_secrets
  where name = 'github_repo_owner'
  limit 1;

  select coalesce(nullif(trim(decrypted_secret), ''), 'ktm-converter') into v_repo
  from vault.decrypted_secrets
  where name = 'github_repo_name'
  limit 1;

  select coalesce(nullif(trim(decrypted_secret), ''), 'main') into v_ref
  from vault.decrypted_secrets
  where name = 'github_repo_ref'
  limit 1;

  if p_mode is null or trim(p_mode) = '' then
    v_inputs := '{}'::jsonb;
  else
    v_inputs := jsonb_build_object('mode', trim(p_mode));
  end if;

  v_url := format(
    'https://api.github.com/repos/%s/%s/actions/workflows/%s/dispatches',
    v_owner,
    v_repo,
    p_workflow_file
  );

  v_request_id := net.http_post(
    url := v_url,
    headers := jsonb_build_object(
      'Authorization', 'Bearer ' || v_pat,
      'Accept', 'application/vnd.github+json',
      'X-GitHub-Api-Version', '2022-11-28',
      'Content-Type', 'application/json'
    ),
    body := jsonb_build_object('ref', v_ref, 'inputs', v_inputs),
    timeout_milliseconds := 15000
  );

  insert into public.workflow_dispatch_log(workflow_file, mode, request_id, status)
  values (p_workflow_file, p_mode, v_request_id, 'queued');

  return v_request_id;
exception
  when others then
    insert into public.workflow_dispatch_log(workflow_file, mode, status, error_message)
    values (p_workflow_file, p_mode, 'failed', left(sqlerrm, 2000));
    raise;
end;
$$;

grant execute on function public.dispatch_github_workflow(text, text) to postgres, service_role;

create or replace function public.dispatch_job_worker_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow('job-worker.yml', null);
$$;

create or replace function public.dispatch_price_eta_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow('price_eta_status_sync.yml', 'apply');
$$;

grant execute on function public.dispatch_job_worker_workflow() to postgres, service_role;
grant execute on function public.dispatch_price_eta_apply_workflow() to postgres, service_role;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_job_worker_hourly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_price_eta_apply_half_hourly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

select cron.schedule(
  'ktm_job_worker_hourly',
  '0 * * * *',
  $$select public.dispatch_job_worker_workflow();$$
);

select cron.schedule(
  'ktm_price_eta_apply_half_hourly',
  '30 * * * *',
  $$select public.dispatch_price_eta_apply_workflow();$$
);
