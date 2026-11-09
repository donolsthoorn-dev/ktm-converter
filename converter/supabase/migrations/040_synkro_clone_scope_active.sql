-- Synkro clone-missing: standaard alle ACTIVE producten (geen Partstream),
-- niet alleen telling-SKU's. Wordt een product later actief, dan pakt de
-- halfuur-run het alsnog.

create or replace function public.dispatch_motox_synkro_clone_missing_workflow()
returns bigint
language sql
security definer
as $$
  select public.dispatch_github_workflow_with_inputs(
    'shops_synkro_clone_missing.yml'::text,
    jsonb_build_object(
      'apply', 'true',
      'max_products', '40',
      'scope', 'active'
    ),
    true
  );
$$;

grant execute on function public.dispatch_motox_synkro_clone_missing_workflow() to postgres, service_role;
