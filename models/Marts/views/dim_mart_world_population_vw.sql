SELECT s2.country_cd,
       s1.country as country_name,
       population,
       yearly_change,
       net_change,
       density,
       land_area,
       migrants,
       fert_rate,
       med_age,
       urban_pop,
       world_share
 FROM {{ ref('dim_mart_world_population') }} s1
 join {{ source("my_src","cntry_lkp")}} s2
   on s1.country=s2.country_name
where s1.end_cal_dim_id='9999-01-01'