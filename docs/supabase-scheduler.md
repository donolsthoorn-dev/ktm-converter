# Supabase Scheduler For GitHub Workflows

This project can trigger both workers from Supabase (`pg_cron + pg_net`) instead of GitHub `schedule`.

## What It Triggers

Alle onderstaande tijden zijn **Europe/Amsterdam** (Nederlandse tijd, met zomer-/wintertijd).  
`pg_cron` draait op **UTC**; migratie `014_github_workflows_cron_europe_amsterdam.sql` vervangt losse UTC-jobs door **één** job `ktm_github_workflows_nl_minutely` (`* * * * *`) die elke minuut `maybe_dispatch_github_workflows_nl_schedule()` aanroept. Die functie dispatcht alleen op de juiste lokale minuut.

| Inhoud (voorheen jobnaam) | Workflow / modus | Lokale tijd |
|---------------------------|------------------|-------------|
| `ktm_job_worker_nightly` | `job-worker.yml` | Dagelijks **03:00** |
| `ktm_shopify_auto_deactivate_after_policy` | `shopify_auto_deactivate_invalid_products.yml` (apply) | Dagelijks **04:00** |
| `ktm_price_eta_apply_hourly_0700_2300` | `price_eta_status_sync.yml` apply, standaard `apply_scope=price_eta` | **07:00** t/m **23:00**, elk heel uur (**:00**) |
| `ktm_price_eta_policy_nightly` | `price_eta_status_sync.yml` apply, `apply_scope=policy` | **00:15**, **07:15**, **12:15**, **18:15** |

- guard: `price_eta` start niet als er al een `scope=policy` run in progress is (tussenliggende uren worden dan bewust overgeslagen)
- zulke overgeslagen runs worden gelogd met `run_state=skipped` en reden `policy_run_in_progress`
- missing-SKU CSV/artifact wordt alleen gemaakt bij `apply_scope=policy` (of `all`)
- policy rerun-resume: als er nog `inventory_policy_changed=true` + `policy_updated_at is null` rows in staging staan, wordt staging rebuild overgeslagen en pakt apply alleen de resterende policy-rows

**Na migratie 014:** de oude cron-jobnamen (`ktm_job_worker_nightly`, enz.) bestaan niet meer in `cron.job`; alleen `ktm_github_workflows_nl_minutely`.

## Why

- More reliable than GitHub schedule minute boundaries
- Centralized scheduling in Supabase (where your queue/data already lives)
- NL-tijden blijven correct bij DST (geen handmatige UTC-aanpassing twee keer per jaar)

## Setup

1. Run migration:
   - `converter/supabase/migrations/006_supabase_cron_dispatch_github_workflows.sql`
   - `converter/supabase/migrations/007_update_supabase_cron_dispatch_times_and_deactivate.sql`
   - `converter/supabase/migrations/009_price_eta_hourly_0700_2300.sql`
   - `converter/supabase/migrations/010_workflow_dispatch_log_run_stats.sql`
   - `converter/supabase/migrations/011_policy_nightly_and_deactivate_after.sql`
   - `converter/supabase/migrations/013_policy_apply_cron_15_0_6_12_18_utc.sql` (historisch; policy-UTC wordt door 014 vervangen)
   - `converter/supabase/migrations/014_github_workflows_cron_europe_amsterdam.sql`
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

Voor compacte run-statistieken (bijv. `price 13 / eta 50 / policy 300`):

```sql
select created_at, workflow_file, mode, run_state, run_summary, run_stats
from public.workflow_dispatch_log
order by created_at desc
limit 50;
```

Check cron jobs (na migratie 014):

Er is **bewust maar één** rij in `cron.job` voor deze scheduler: `ktm_github_workflows_nl_minutely` (`* * * * *`). De vier “oude” jobnamen (job worker, price/ETA, policy, deactivate) bestaan niet meer als aparte cron-rijen; de **tijden** staan in `public.maybe_dispatch_github_workflows_nl_schedule()`.

```sql
select jobid, jobname, schedule, command, active
from cron.job
where jobname = 'ktm_github_workflows_nl_minutely';
```

Alle `ktm_`-cronjobs in één keer (handig vóór/na migratie, of om oude rijen te spotten):

```sql
select jobid, jobname, schedule, command, active
from cron.job
where jobname like 'ktm_%'
order by jobname;
```

## Notes

- GitHub workflow files are now `workflow_dispatch`-driven only.
- Supabase scheduler dispatches to GitHub via API.
- Keep all existing GitHub Secrets in place; Supabase only triggers the run.
