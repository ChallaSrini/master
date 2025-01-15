{{ config(
    materialized='incremental',
    incremental_strategy='merge', 
    unique_key='dim_cntry_key', 
    merge_update_columns=['country','population','yearly_change','net_change','density','land_area','migrants','fert_rate','med_age','urban_pop','world_share','last_updt_dt','load_datetime','compare_key'],
    on_schema_change='fail' 
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
       batch_dt,
       last_updt_dt,
       load_datetime,
       compare_key
FROM {{ ref('wrk_mart_world_population_1') }}