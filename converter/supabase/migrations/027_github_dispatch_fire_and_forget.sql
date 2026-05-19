-- 026: COMMIT in procedure faalt via SELECT/CALL uit functie (2D000 invalid transaction termination).
-- pg_net: request start na COMMIT van de aanroepende transactie → geen poll/collect in dezelfde functie.
-- Dispatch: queue + status queued; GitHub-workflow koppelt via dispatch_log_id.

drop procedure if exists public._dispatch_github_workflow_with_inputs_impl(text, jsonb, boolean, bigint);
drop procedure if exists public._debug_pg_net_github_ping_impl(text);

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

  perform net.wake();

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
        error_message = left(coalesce(sqlerrm, 'github_dispatch failed'), 2000),
        run_finished_at = coalesce(run_finished_at, now())
      where id = v_log_id
        and status not in ('failed', 'skipped');
    end if;
    raise;
end;
$$;

grant execute on function public.dispatch_github_workflow_with_inputs(text, jsonb, boolean)
  to postgres, service_role;

-- Diagnose: queue request; na COMMIT (einde query) verwerkt pg_net. Antwoord in tweede query.
create or replace function public.debug_pg_net_github_ping()
returns text
language plpgsql
security definer
set search_path = public, net
as $$
declare
  v_id bigint;
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
  return format(
    'queued request_id=%s. Wacht 3s, run daarna: select status_code, left(coalesce(content, error_msg), 200) from net._http_response where id = %s;',
    v_id,
    v_id
  );
end;
$$;

grant execute on function public.debug_pg_net_github_ping() to postgres, service_role;
