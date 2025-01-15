select *
  from {{ ref('dim_mart_world_population') }}
 where effective_to='9999-01-01'