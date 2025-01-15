{{ config(materialized='table') }}

with wrld_popln as (
    select upper(md5(country)) as dim_cntry_key,
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
           cast('{{ var("buss_dt",'9999-01-01') }}' as date) as effective_from,
           cast('9999-01-01' as date) as effective_to,
           CURRENT_TIMESTAMP() as load_datetime,
           upper(md5(concat_ws(',',country,effective_from))) as scd_key,
           upper(md5(concat_ws(',',population,yearly_change,net_change,density,land_area,migrants,fert_rate,med_age,urban_pop,world_share))) as compare_key
      from ic4_catalog.wrk_training.world_population
)

select * from wrld_popln
