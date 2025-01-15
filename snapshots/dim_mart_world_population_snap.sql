{% snapshot dim_mart_world_population_snap %}

{{ config(
    materialized='snapshot',
    unique_key='dim_cntry_key',
    strategy='check',
    invalidate_hard_deletes=False,
    check_cols=['compare_key'] 
) }}

SELECT dim_cntry_key,
       country,
       population,
       yearly_change,
       net_change,
       density,
       land_area,
       migrants,
       fert_rate,
       med_age,
       urban_pop,
       world_share,
       compare_key
FROM {{ ref('wrk_mart_world_population') }}

{% endsnapshot %}