-- Build a filter/backfill-friendly projection from shopify_ymm.ymm_json.
-- Source JSON shape expected:
--   { "MAKE": { "MODEL": ["2012","2013"], ... }, ... }

create table if not exists public.shopify_ymm_projection (
  shopify_product_id bigint primary key references public.shopify_products (shopify_product_id) on delete cascade,
  sku text,
  fits_on_make_old text,
  fits_on_model_old text,
  fits_on_year_old text,
  fits_on_make_new jsonb not null default '[]'::jsonb,
  fits_on_model_new jsonb not null default '[]'::jsonb,
  fits_on_year_new jsonb not null default '[]'::jsonb,
  ymm_summary text,
  source_synced_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists shopify_ymm_projection_sku_idx
  on public.shopify_ymm_projection (sku);

create index if not exists shopify_ymm_projection_updated_at_idx
  on public.shopify_ymm_projection (updated_at desc);

alter table public.shopify_ymm_projection enable row level security;

create or replace function public.refresh_shopify_ymm_projection()
returns jsonb
language plpgsql
as $$
declare
  v_rows integer := 0;
begin
  -- Deze refresh kan op grote catalogi zwaar zijn; voorkom statement timeout.
  perform set_config('statement_timeout', '0', true);

  with
  source_rows as (
    select
      y.shopify_product_id,
      y.ymm_json,
      y.synced_at
    from public.shopify_ymm y
    where y.ymm_json is not null
      and jsonb_typeof(y.ymm_json) = 'object'
  ),
  flattened as (
    select
      s.shopify_product_id,
      upper(trim(both from mk.key)) as make,
      upper(trim(both from md.key)) as model,
      upper(trim(both from yr.value)) as year,
      s.synced_at
    from source_rows s
    cross join lateral jsonb_each(s.ymm_json) mk
    cross join lateral jsonb_each(mk.value) md
    cross join lateral jsonb_array_elements_text(md.value) yr(value)
    where trim(both from mk.key) <> ''
      and trim(both from md.key) <> ''
      and trim(both from yr.value) <> ''
  ),
  agg as (
    select
      f.shopify_product_id,
      (array_agg(distinct f.make order by f.make))[1:128] as makes,
      (array_agg(distinct f.model order by f.model))[1:128] as models,
      (array_agg(distinct f.year order by f.year))[1:128] as years,
      min(case when f.year ~ '^\d{4}$' then f.year::int end) as year_min,
      max(case when f.year ~ '^\d{4}$' then f.year::int end) as year_max,
      max(f.synced_at) as source_synced_at
    from flattened f
    group by f.shopify_product_id
  ),
  sku_one as (
    select
      v.shopify_product_id,
      min(nullif(trim(v.sku), '')) as sku
    from public.shopify_variants v
    group by v.shopify_product_id
  ),
  upserted as (
    insert into public.shopify_ymm_projection (
      shopify_product_id,
      sku,
      fits_on_make_old,
      fits_on_model_old,
      fits_on_year_old,
      fits_on_make_new,
      fits_on_model_new,
      fits_on_year_new,
      ymm_summary,
      source_synced_at,
      updated_at
    )
    select
      a.shopify_product_id,
      s.sku,
      array_to_string(a.makes, '||') as fits_on_make_old,
      array_to_string(a.models, '||') as fits_on_model_old,
      array_to_string(a.years, '||') as fits_on_year_old,
      to_jsonb(a.makes) as fits_on_make_new,
      to_jsonb(a.models) as fits_on_model_new,
      to_jsonb(a.years) as fits_on_year_new,
      case
        when cardinality(a.makes) > 0 and a.year_min is not null and a.year_max is not null
          then array_to_string(a.makes, ', ') || ' ' || a.year_min::text || '-' || a.year_max::text
        when cardinality(a.makes) > 0
          then array_to_string(a.makes, ', ')
        else null
      end as ymm_summary,
      a.source_synced_at,
      now() as updated_at
    from agg a
    left join sku_one s on s.shopify_product_id = a.shopify_product_id
    on conflict (shopify_product_id) do update
      set
        sku = excluded.sku,
        fits_on_make_old = excluded.fits_on_make_old,
        fits_on_model_old = excluded.fits_on_model_old,
        fits_on_year_old = excluded.fits_on_year_old,
        fits_on_make_new = excluded.fits_on_make_new,
        fits_on_model_new = excluded.fits_on_model_new,
        fits_on_year_new = excluded.fits_on_year_new,
        ymm_summary = excluded.ymm_summary,
        source_synced_at = excluded.source_synced_at,
        updated_at = excluded.updated_at
    returning 1
  )
  select count(*) into v_rows from upserted;

  return jsonb_build_object(
    'ok', true,
    'upserted_rows', v_rows,
    'at', now()
  );
end;
$$;
