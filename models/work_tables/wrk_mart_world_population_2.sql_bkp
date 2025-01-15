
{{ config(materialized='table') }}

 with wrld_scd2 as (
     select s1.dim_cntry_key,
			s1.country,
			s1.population,
			s1.yearly_change,
			s1.net_change,
			s1.density,
			s1.land_area,
			s1.migrants,
			s1.fert_rate,
			s1.med_age,
			s1.urban_pop,
			s1.world_share,
			s1.effective_from,
			s1.effective_to,
			s1.load_datetime,
			s1.scd_key,
			s1.compare_key 
       from {{ ref('wrk_mart_world_population') }} s1
  left join ic4_catalog.wrk_training.dim_mart_world_population s2
         on s1.dim_cntry_key=s2.dim_cntry_key
        and s2.effective_to='9999-01-01'
      where s2.dim_cntry_key is null
         or (s1.compare_key != s2.compare_key) 
      union all
     select s2.dim_cntry_key,
			s2.country,
			s2.population,
			s2.yearly_change,
			s2.net_change,
			s2.density,
			s2.land_area,
			s2.migrants,
			s2.fert_rate,
			s2.med_age,
			s2.urban_pop,
			s2.world_share,
			s2.effective_from,
			s1.effective_from - INTERVAL 1 DAY as effective_to,
			s2.load_datetime,
			s2.scd_key,
			s2.compare_key 
       from {{ ref('wrk_mart_world_population') }} s1
       join ic4_catalog.wrk_training.dim_mart_world_population s2
         on s1.dim_cntry_key=s2.dim_cntry_key
        and s2.effective_to='9999-01-01'
      where s1.compare_key != s2.compare_key 	 )

select * from wrld_scd2  