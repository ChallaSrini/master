--delete+insert and append different incremental_strategy 
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
       effective_from,
       effective_to,
       load_datetime,
       scd_key,
       compare_key
FROM {{ ref('wrk_mart_world_population_2') }}
