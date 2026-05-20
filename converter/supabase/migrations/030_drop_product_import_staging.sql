-- Terug naar handmatige Shopify-import (Admin). Verwijder staging-tabellen uit 029.

drop table if exists public.product_import_staging;
drop table if exists public.product_import_batch;
