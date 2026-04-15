-- Update Supabase-driven scheduling for GitHub workflows.
--
-- Target schedules (UTC):
--   - job-worker.yml: daily at 03:00
--   - price_eta_status_sync.yml (apply): every 3 hours starting at 04:00
--     -> 04:00, 07:00, 10:00, 13:00, 16:00, 19:00, 22:00, 01:00
--   - shopify_auto_deactivate_invalid_products.yml (apply=true): daily at 05:00

create extension if not exists pg_net;
create extension if not exists pg_cron;
create extension if not exists supabase_vault;

create or replace function public.dispatch_github_workflow_with_inputs(
  p_workflow_file text,
  p_inputs jsonb default '{}'::jsonb
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
  v_mode text;
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
    body := jsonb_build_object('ref', v_ref, 'inputs', coalesce(p_inputs, '{}'::jsonb)),
    timeout_milliseconds := 15000
  );

  v_mode := nullif(trim(coalesce(p_inputs ->> 'mode', '')), '');
  if v_mode is null then
    v_mode := nullif(trim(coalesce(p_inputs ->> 'apply', '')), '');
  end if;

  insert into public.workflow_dispatch_log(workflow_file, mode, request_id, status)
  values (p_workflow_file, v_mode, v_request_id, 'queued');

  return v_request_id;
exception
  when others then
    insert into public.workflow_dispatch_log(workflow_file, mode, status, error_message)
    values (p_workflow_file, null, 'failed', left(sqlerrm, 2000));
    raise;
end;
$$;

grant execute on function public.dispatch_github_workflow_with_inputs(text, jsonb) to postgres, service_role;

create or replace function public.dispatch_job_worker_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs('job-worker.yml', '{}'::jsonb);
$$;

create or replace function public.dispatch_price_eta_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'price_eta_status_sync.yml',
    jsonb_build_object('mode', 'apply')
  );
$$;

create or replace function public.dispatch_shopify_auto_deactivate_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shopify_auto_deactivate_invalid_products.yml',
    jsonb_build_object('apply', 'true')
  );
$$;

grant execute on function public.dispatch_job_worker_workflow() to postgres, service_role;
grant execute on function public.dispatch_price_eta_apply_workflow() to postgres, service_role;
grant execute on function public.dispatch_shopify_auto_deactivate_apply_workflow() to postgres, service_role;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_job_worker_nightly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_price_eta_apply_3hourly';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

do $$
declare
  v_jobid bigint;
begin
  select jobid into v_jobid from cron.job where jobname = 'ktm_shopify_auto_deactivate_0500';
  if v_jobid is not null then
    perform cron.unschedule(v_jobid);
  end if;
end $$;

select cron.schedule(
  'ktm_job_worker_nightly',
  '0 3 * * *',
  $$select public.dispatch_job_worker_workflow();$$
);

select cron.schedule(
  'ktm_price_eta_apply_3hourly',
  '0 1,4,7,10,13,16,19,22 * * *',
  $$select public.dispatch_price_eta_apply_workflow();$$
);

select cron.schedule(
  'ktm_shopify_auto_deactivate_0500',
  '0 5 * * *',
  $$select public.dispatch_shopify_auto_deactivate_apply_workflow();$$
);
