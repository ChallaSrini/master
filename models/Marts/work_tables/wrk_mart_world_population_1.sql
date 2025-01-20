 {% if relation_exists(source("my_src", "tgt_population_typ1")) %}
 with wrld_scd1 as (
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
			s1.eff_cal_dim_id as batch_dt,
			s1.eff_cal_dim_id as last_updt_dt,
			coalesce(s2.inst_ts,s1.inst_ts) as inst_ts,
            case when s2.dim_cntry_key is null 
                 then cast(null as timestamp)
                 else s1.inst_ts
             end as updt_ts,
			s1.compare_key
       from {{ ref('wrk_mart_world_population') }} s1
  left join {{source("my_src","tgt_population_typ1")}} s2
         on s1.dim_cntry_key=s2.dim_cntry_key
      where s2.dim_cntry_key is null
         or (s1.compare_key != s2.compare_key) 
                     )
{% else %}
 with wrld_scd1 as (
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
			s1.eff_cal_dim_id as batch_dt,
			s1.eff_cal_dim_id as last_updt_dt,
			s1.inst_ts as inst_ts,
            cast(null as timestamp) as updt_ts,
			s1.compare_key
       from {{ ref('wrk_mart_world_population') }} s1
         )
{% endif %}         
 select * from wrld_scd1 