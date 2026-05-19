-- Valideer GitHub workflow_dispatch HTTP-antwoord (pg_net); anders blijft status ten onrechte 'queued'.

create or replace function public.await_net_http_response(
  p_request_id bigint,
  p_max_wait_ms integer default 10000,
  p_poll_ms integer default 200
)
returns table(status_code integer, body_text text, timed_out boolean)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_elapsed integer := 0;
  v_code integer;
  v_body text;
  v_timed_out boolean;
begin
  if p_request_id is null then
    return query select null::integer, 'missing request_id'::text, true;
    return;
  end if;

  loop
    select
      r.status_code,
      coalesce(r.content, r.error_msg, ''),
      coalesce(r.timed_out, false)
    into v_code, v_body, v_timed_out
    from net._http_response r
    where r.id = p_request_id;

    if found then
      return query select v_code, left(v_body, 4000), v_timed_out;
      return;
    end if;

    exit when v_elapsed >= greatest(p_max_wait_ms, 500);
    perform pg_sleep(greatest(p_poll_ms, 50) / 1000.0);
    v_elapsed := v_elapsed + greatest(p_poll_ms, 50);
  end loop;

  return query select null::integer, ''::text, true;
end;
$$;

grant execute on function public.await_net_http_response(bigint, integer, integer) to postgres, service_role;

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
  v_http_status integer;
  v_http_body text;
  v_http_timeout boolean;
  v_err text;
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

  select h.status_code, h.body_text, h.timed_out
  into v_http_status, v_http_body, v_http_timeout
  from public.await_net_http_response(v_request_id, 10000, 200) h;

  if coalesce(v_http_timeout, false) then
    v_err := 'github_dispatch: geen HTTP-antwoord (timeout)';
    update public.workflow_dispatch_log
    set
      request_id = v_request_id,
      status = 'failed',
      run_state = 'failed',
      error_message = v_err,
      run_finished_at = now()
    where id = v_log_id;
    raise exception '%', v_err;
  end if;

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
      request_id = v_request_id,
      status = 'failed',
      run_state = 'failed',
      error_message = v_err,
      run_finished_at = now()
    where id = v_log_id;
    raise exception '%', v_err;
  end if;

  update public.workflow_dispatch_log
  set request_id = v_request_id, status = 'queued'
  where id = v_log_id;

  return v_request_id;
exception
  when others then
    if v_log_id is not null then
      update public.workflow_dispatch_log
      set
        status = 'failed',
        run_state = 'failed',
        error_message = left(
          coalesce(sqlerrm, 'github_dispatch failed'),
          2000
        ),
        run_finished_at = coalesce(run_finished_at, now())
      where id = v_log_id
        and status <> 'failed';
    else
      insert into public.workflow_dispatch_log(workflow_file, mode, status, error_message)
      values (p_workflow_file, v_mode, 'failed', left(sqlerrm, 2000));
    end if;
    raise;
end;
$$;

grant execute on function public.dispatch_github_workflow_with_inputs(text, jsonb, boolean)
  to postgres, service_role;
