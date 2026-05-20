-- Canonical fitment uit XML (bron van waarheid). Shopify-mirror vult dit NIET meer.
-- Push-job: alleen rijen waar content_hash <> pushed_hash (of pushed_hash is null).

create table if not exists public.canonical_product_fits_on (
  shopify_product_id bigint primary key references public.shopify_products (shopify_product_id) on delete cascade,
  ymm_json jsonb not null,
  content_hash text not null,
  pushed_hash text,
  xml_synced_at timestamptz not null default now(),
  pushed_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists canonical_product_fits_on_content_hash_idx
  on public.canonical_product_fits_on (content_hash);

create index if not exists canonical_product_fits_on_pending_idx
  on public.canonical_product_fits_on (shopify_product_id)
  where pushed_hash is null or pushed_hash is distinct from content_hash;

alter table public.canonical_product_fits_on enable row level security;

comment on table public.canonical_product_fits_on is
  'YMM/fits_on uit CBEXPDN XML (cross-brand). Shopify catalog mirror schrijft hier niet naar.';

-- Projection leest canonical (niet shopify_ymm uit Shopify-spiegel).
create or replace function public.refresh_shopify_ymm_projection()
returns jsonb
language plpgsql
as $$
declare
  v_rows integer := 0;
begin
  perform set_config('statement_timeout', '0', true);

  with
  source_rows as (
    select
      c.shopify_product_id,
      c.ymm_json,
      c.xml_synced_at as synced_at
    from public.canonical_product_fits_on c
    where c.ymm_json is not null
      and jsonb_typeof(c.ymm_json) = 'object'
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
    'source', 'canonical_product_fits_on',
    'at', now()
  );
end;
$$;

alter function public.refresh_shopify_ymm_projection() set statement_timeout = '0';
