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
       eff_cal_dim_id,
       end_cal_dim_id,
       inst_ts,
       scd_key,
       compare_key
FROM {{ ref('wrk_mart_world_population_2') }}
