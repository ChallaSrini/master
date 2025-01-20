{{ config(
    unique_key='dim_cntry_key', 
    merge_update_columns=['country','population','yearly_change','net_change','density','land_area','migrants','fert_rate','med_age','urban_pop','world_share','updt_ts','last_updt_dt','compare_key']
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
       inst_ts,
       updt_ts,
       compare_key
FROM {{ ref('wrk_mart_world_population_1') }}