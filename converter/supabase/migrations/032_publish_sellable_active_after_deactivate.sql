-- Stap 5: publiceer ACTIVE producten op Webshop (published_at leeg + ERP verkoopbaar).
-- Planning NL: dagelijks 04:30, na auto_deactivate (04:00), vóór customs (05:00).
--
-- Deze migratie is zelfstandig uitvoerbaar als dispatch_github_workflow_with_inputs
-- nog niet op Supabase staat (bijv. alleen migratie 006/018 gedraaid).

create extension if not exists pg_net;
create extension if not exists pg_cron;
create extension if not exists supabase_vault;

create table if not exists public.workflow_dispatch_log (
  id bigint generated always as identity primary key,
  created_at timestamptz not null default now(),
  workflow_file text not null,
  mode text,
  request_id bigint,
  status text not null,
  error_message text
);

alter table public.workflow_dispatch_log enable row level security;

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

grant execute on function public.dispatch_github_workflow_with_inputs(text, jsonb)
  to postgres, service_role;

create or replace function public.dispatch_shopify_auto_deactivate_apply_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shopify_auto_deactivate_invalid_products.yml'::text,
    jsonb_build_object('apply', 'true')
  );
$$;

grant execute on function public.dispatch_shopify_auto_deactivate_apply_workflow() to postgres, service_role;

create or replace function public.dispatch_shopify_publish_sellable_active_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shopify_publish_sellable_active_products.yml'::text,
    jsonb_build_object('apply', 'true')
  );
$$;

grant execute on function public.dispatch_shopify_publish_sellable_active_workflow() to postgres, service_role;

create or replace function public.maybe_dispatch_github_workflows_nl_schedule()
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  nl timestamp without time zone := (now() at time zone 'Europe/Amsterdam');
  h int := extract(hour from nl)::int;
  m int := extract(minute from nl)::int;
begin
  if h = 3 and m = 0 then
    perform public.dispatch_job_worker_workflow();
  end if;

  if h = 4 and m = 0 then
    perform public.dispatch_shopify_auto_deactivate_apply_workflow();
  end if;

  if h = 4 and m = 30 then
    perform public.dispatch_shopify_publish_sellable_active_workflow();
  end if;

  if h = 5 and m = 0 then
    perform public.dispatch_customs_missing_fill_workflow();
  end if;

  if h between 7 and 23 and m = 0 then
    perform public.dispatch_price_eta_apply_workflow();
  end if;

  if m = 15 and h in (0, 7, 12, 18) then
    perform public.dispatch_price_eta_policy_workflow();
  end if;
end;
$$;

grant execute on function public.maybe_dispatch_github_workflows_nl_schedule() to postgres, service_role;
