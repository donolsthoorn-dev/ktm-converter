-- Review-status vervalt: staging is nu een volledige momentopname per run.
-- Definitieve apply-logica beslist zelf wat geüpdatet wordt.

drop index if exists public.pricelist_sync_staging_review_idx;

alter table if exists public.pricelist_sync_staging
  drop column if exists review_status;
