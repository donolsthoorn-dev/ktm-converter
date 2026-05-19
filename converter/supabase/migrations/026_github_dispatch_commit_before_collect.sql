-- pg_net start HTTP pas na COMMIT. Poll/collect in dezelfde transactie (025) faalt altijd.
-- Oplossing: COMMIT na http_post, daarna net.http_collect_response in dezelfde procedure.

create or replace procedure public._dispatch_github_workflow_with_inputs_impl(
  p_workflow_file text,
  p_inputs jsonb default '{}'::jsonb,
  p_requires_shopify_lock boolean default false,
  inout p_request_id bigint default null
)
language plpgsql
security definer
set search_path = public, vault, net
as $$
declare
  v_owner text;
  v_repo text;
  v_ref text;
  v_pat text;
  v_url text;
  v_mode text;
  v_log_id bigint;
  v_inputs jsonb;
  v_http_status integer;
  v_http_body text;
  v_err text;
  v_collect net.http_response_result;
  v_resp net.http_response;
begin
  perform public.mark_stale_workflow_dispatch_logs();

  begin
    perform net.check_worker_is_up();
  exception
    when others then
      insert into public.workflow_dispatch_log(
        workflow_file, mode, status, run_state, run_summary, error_message, run_finished_at
      )
      values (
        p_workflow_file,
        nullif(trim(coalesce(p_inputs ->> 'mode', p_inputs ->> 'apply', '')), ''),
        'failed',
        'failed',
        'pg_net_worker_down',
        left(sqlerrm, 2000),
        now()
      );
      raise;
  end;

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
    p_request_id := null;
    return;
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

  p_request_id := net.http_post(
    url := v_url,
    body := jsonb_build_object('ref', v_ref, 'inputs', v_inputs),
    params := '{}'::jsonb,
    headers := jsonb_build_object(
      'Authorization', 'Bearer ' || v_pat,
      'Accept', 'application/vnd.github+json',
      'X-GitHub-Api-Version', '2022-11-28',
      'Content-Type', 'application/json'
    ),
    timeout_milliseconds := 20000
  );

  update public.workflow_dispatch_log
  set request_id = p_request_id
  where id = v_log_id;

  perform net.wake();
  commit;

  set local statement_timeout = '20s';
  v_collect := net.http_collect_response(p_request_id, false);

  if v_collect.status is distinct from 'SUCCESS' then
    v_err := left(
      coalesce(
        nullif(trim(v_collect.message), ''),
        format('github_dispatch collect status=%s', coalesce(v_collect.status::text, '?'))
      ),
      2000
    );
    update public.workflow_dispatch_log
    set
      status = 'failed',
      run_state = 'failed',
      error_message = v_err,
      run_finished_at = now()
    where id = v_log_id;
    raise exception '%', v_err;
  end if;

  v_resp := v_collect.response;
  v_http_status := v_resp.status_code;
  v_http_body := coalesce(v_resp.body, '');

  if coalesce(v_http_status, 0) <> 204 then
    v_err := left(
      format(
        'github_dispatch HTTP %s: %s',
        coalesce(v_http_status::text, '?'),
        coalesce(nullif(trim(v_http_body), ''), '(leeg)')
      ),
      2000
    );
    update public.workflow_dispatch_log
    set
      status = 'failed',
      run_state = 'failed',
      error_message = v_err,
      run_finished_at = now()
    where id = v_log_id;
    raise exception '%', v_err;
  end if;

  update public.workflow_dispatch_log
  set status = 'queued'
  where id = v_log_id;
exception
  when others then
    if v_log_id is not null then
      update public.workflow_dispatch_log
      set
        status = 'failed',
        run_state = 'failed',
        error_message = left(coalesce(sqlerrm, 'github_dispatch failed'), 2000),
        run_finished_at = coalesce(run_finished_at, now())
      where id = v_log_id
        and status not in ('failed', 'skipped');
    end if;
    raise;
end;
$$;

grant execute on procedure public._dispatch_github_workflow_with_inputs_impl(text, jsonb, boolean, bigint)
  to postgres, service_role;

create or replace function public.dispatch_github_workflow_with_inputs(
  p_workflow_file text,
  p_inputs jsonb default '{}'::jsonb,
  p_requires_shopify_lock boolean default false
)
returns bigint
language plpgsql
security definer
set search_path = public, vault, net
as $$
declare
  v_request_id bigint;
begin
  call public._dispatch_github_workflow_with_inputs_impl(
    p_workflow_file,
    p_inputs,
    p_requires_shopify_lock,
    v_request_id
  );
  return v_request_id;
end;
$$;

grant execute on function public.dispatch_github_workflow_with_inputs(text, jsonb, boolean)
  to postgres, service_role;

create or replace procedure public._debug_pg_net_github_ping_impl(out p_result text)
language plpgsql
security definer
set search_path = public, net
as $$
declare
  v_id bigint;
  v_collect net.http_response_result;
  v_resp net.http_response;
begin
  perform net.check_worker_is_up();
  v_id := net.http_post(
    url := 'https://api.github.com/zen',
    body := '{}'::jsonb,
    params := '{}'::jsonb,
    headers := '{"Content-Type": "application/json"}'::jsonb,
    timeout_milliseconds := 5000
  );
  perform net.wake();
  commit;

  set local statement_timeout = '10s';
  v_collect := net.http_collect_response(v_id, false);

  if v_collect.status is distinct from 'SUCCESS' then
    p_result := format(
      'FAIL: collect status=%s msg=%s',
      coalesce(v_collect.status::text, '?'),
      left(coalesce(v_collect.message, ''), 200)
    );
    return;
  end if;

  v_resp := v_collect.response;
  p_result := format(
    'OK: HTTP %s body=%s',
    coalesce(v_resp.status_code::text, '?'),
    left(coalesce(v_resp.body, ''), 200)
  );
end;
$$;

grant execute on procedure public._debug_pg_net_github_ping_impl(text) to postgres, service_role;

create or replace function public.debug_pg_net_github_ping()
returns text
language plpgsql
security definer
set search_path = public, net
as $$
declare
  v_result text;
begin
  call public._debug_pg_net_github_ping_impl(v_result);
  return v_result;
end;
$$;

grant execute on function public.debug_pg_net_github_ping() to postgres, service_role;
