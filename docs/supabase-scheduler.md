# Supabase Scheduler For GitHub Workflows

This project can trigger both workers from Supabase (`pg_cron + pg_net`) instead of GitHub `schedule`.

## What It Triggers

- `job-worker.yml` daily at `02:10 UTC` (`10 2 * * *`)
- `price_eta_status_sync.yml` in `apply` mode every 3 hours at minute `:30` (`30 */3 * * *`)

## Why

- More reliable than GitHub schedule minute boundaries
- Centralized scheduling in Supabase (where your queue/data already lives)

## Setup

1. Run migration:
   - `converter/supabase/migrations/006_supabase_cron_dispatch_github_workflows.sql`
2. Create vault secret for GitHub API token (required):

```sql
select vault.create_secret(
  '<github_pat_with_actions_write>',
  'github_actions_pat',
  'PAT for workflow dispatch'
);
```

3. Optional vault overrides (defaults shown):

```sql
select vault.create_secret('donolsthoorn-dev', 'github_repo_owner', 'Repo owner');
select vault.create_secret('ktm-converter', 'github_repo_name', 'Repo name');
select vault.create_secret('main', 'github_repo_ref', 'Git ref');
```

## Token Permissions

- Classic PAT: `repo`, `workflow`
- Fine-grained PAT: Actions (read/write) + Contents (read/write) on this repo

## Monitoring

Check Supabase dispatch logs:

```sql
select *
from public.workflow_dispatch_log
order by created_at desc
limit 100;
```

Check cron jobs:

```sql
select jobid, jobname, schedule, command, active
from cron.job
where jobname in ('ktm_job_worker_nightly', 'ktm_price_eta_apply_3hourly');
```

## Notes

- GitHub workflow files are now `workflow_dispatch`-driven only.
- Supabase scheduler dispatches to GitHub via API.
- Keep all existing GitHub Secrets in place; Supabase only triggers the run.
