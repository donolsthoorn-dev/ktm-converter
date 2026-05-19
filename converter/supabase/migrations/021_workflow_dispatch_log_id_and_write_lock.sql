-- Workflow dispatch: koppel GitHub-run aan de juiste logrij (dispatch_log_id),
-- sla dispatch over bij bezette shopify-write-lock, ruim verweesde queued-rijen op,
-- en stuur apply_scope=price_eta expliciet mee.

drop function if exists public.dispatch_github_workflow_with_inputs(text, jsonb);

create or replace function public.shopify_write_lock_is_busy()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.workflow_dispatch_log w
    where w.workflow_file in (
      'price_eta_status_sync.yml',
      'customs_missing_fill.yml',
      'shopify_auto_deactivate_invalid_products.yml'
    )
      and w.run_state = 'running'
      and w.run_started_at > now() - interval '6 hours'
  );
$$;

grant execute on function public.shopify_write_lock_is_busy() to postgres, service_role;

create or replace function public.mark_stale_workflow_dispatch_logs()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer;
begin
  update public.workflow_dispatch_log w
  set
    status = 'skipped',
    run_state = 'skipped',
    run_summary = coalesce(w.run_summary, 'dispatch_never_started'),
    run_finished_at = coalesce(w.run_finished_at, now())
  where w.status = 'queued'
    and w.github_run_id is null
    and (w.run_state is null or w.run_state = '')
    and w.created_at < now() - interval '2 hours'
    and w.workflow_file like '%.yml';

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

grant execute on function public.mark_stale_workflow_dispatch_logs() to postgres, service_role;

create or replace function public.dispatch_github_workflow_with_inputs(
  p_workflow_file text,
  p_inputs jsonb default '{}'::jsonb,
  p_requires_shopify_lock boolean default false
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
  v_log_id bigint;
  v_inputs jsonb;
begin
  perform public.mark_stale_workflow_dispatch_logs();

  v_mode := nullif(trim(coalesce(p_inputs ->> 'mode', '')), '');
  if v_mode is null then
    v_mode := nullif(trim(coalesce(p_inputs ->> 'apply', '')), '');
  end if;

  if coalesce(p_requires_shopify_lock, false) and public.shopify_write_lock_is_busy() then
    insert into public.workflow_dispatch_log(
      workflow_file, mode, status, run_state, run_summary, run_finished_at
    )
    values (
      p_workflow_file,
      v_mode,
      'skipped',
      'skipped',
      'shopify_write_lock_busy',
      now()
    );
    return null;
  end if;

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

  insert into public.workflow_dispatch_log(workflow_file, mode, status)
  values (p_workflow_file, v_mode, 'pending')
  returning id into v_log_id;

  v_inputs := coalesce(p_inputs, '{}'::jsonb)
    || jsonb_build_object('dispatch_log_id', v_log_id::text);

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

  update public.workflow_dispatch_log
  set request_id = v_request_id, status = 'queued'
  where id = v_log_id;

  return v_request_id;
exception
  when others then
    if v_log_id is not null then
      update public.workflow_dispatch_log
      set status = 'failed', run_state = 'failed', error_message = left(sqlerrm, 2000)
      where id = v_log_id;
    else
      insert into public.workflow_dispatch_log(workflow_file, mode, status, error_message)
      values (p_workflow_file, v_mode, 'failed', left(sqlerrm, 2000));
    end if;
    raise;
end;
$$;

grant execute on function public.dispatch_github_workflow_with_inputs(text, jsonb, boolean)
  to postgres, service_role;

create or replace function public.dispatch_github_workflow(
  p_workflow_file text,
  p_mode text default null
)
returns bigint
language plpgsql
security definer
as $$
declare
  v_inputs jsonb := '{}'::jsonb;
  v_requires_lock boolean := p_workflow_file in (
    'price_eta_status_sync.yml',
    'customs_missing_fill.yml',
    'shopify_auto_deactivate_invalid_products.yml'
  );
begin
  if p_mode is not null and trim(p_mode) <> '' then
    v_inputs := jsonb_build_object('mode', trim(p_mode));
  end if;
  return public.dispatch_github_workflow_with_inputs(
    p_workflow_file,
    v_inputs,
    v_requires_lock
  );
end;
$$;

create or replace function public.dispatch_job_worker_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs('job-worker.yml', '{}'::jsonb, false);
$$;

create or replace function public.dispatch_price_eta_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'price_eta_status_sync.yml',
    jsonb_build_object('mode', 'apply', 'apply_scope', 'price_eta'),
    true
  );
$$;

create or replace function public.dispatch_price_eta_policy_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'price_eta_status_sync.yml',
    jsonb_build_object('mode', 'apply', 'apply_scope', 'policy'),
    true
  );
$$;

create or replace function public.dispatch_shopify_auto_deactivate_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shopify_auto_deactivate_invalid_products.yml',
    jsonb_build_object('apply', 'true'),
    true
  );
$$;

create or replace function public.dispatch_customs_missing_fill_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'customs_missing_fill.yml',
    '{}'::jsonb,
    true
  );
$$;

grant execute on function public.dispatch_job_worker_workflow() to postgres, service_role;
grant execute on function public.dispatch_price_eta_apply_workflow() to postgres, service_role;
grant execute on function public.dispatch_price_eta_policy_workflow() to postgres, service_role;
grant execute on function public.dispatch_shopify_auto_deactivate_apply_workflow() to postgres, service_role;
grant execute on function public.dispatch_customs_missing_fill_workflow() to postgres, service_role;
