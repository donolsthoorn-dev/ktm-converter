-- PostgREST/Supabase default statement_timeout (~8s) kills refresh_shopify_ymm_projection.
-- set_config inside the function is not always enough; this sets it on the function itself.

alter function public.refresh_shopify_ymm_projection() set statement_timeout = '0';
