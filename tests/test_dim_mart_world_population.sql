select dim_cntry_key 
  from ( 
select dim_cntry_key,
       eff_cal_dim_id,
       count(*)
  from {{ ref('dim_mart_world_population') }}
 group by 1,2 
 having count(*)>1
        ) 
    union all 
select scd_key
  from {{ ref('dim_mart_world_population') }}
 group by 1
 having count(*)>1