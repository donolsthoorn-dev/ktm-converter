-- Geplande digest om 10:00: vast venster (until_now=false).
-- Handmatige runs gebruiken workflow-default until_now=true.

create or replace function public.dispatch_daily_actions_digest_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'daily_actions_digest.yml'::text,
    jsonb_build_object(
      'send', 'true',
      'until_now', 'false'
    ),
    false
  );
$$;

grant execute on function public.dispatch_daily_actions_digest_workflow() to postgres, service_role;
