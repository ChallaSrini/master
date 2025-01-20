 {% if relation_exists(source("my_src", "tgt_population")) %}
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
			s1.eff_cal_dim_id,
			s1.end_cal_dim_id,
			s1.inst_ts,
			s1.scd_key,
			s1.compare_key,
            s2.scd_key as upd_scd_key,
            s1.eff_cal_dim_id - INTERVAL 1 DAY as upd_end_cal_dim_id
       from {{ ref('wrk_mart_world_population') }} s1
  left join {{source("my_src","tgt_population")}} s2
         on s1.dim_cntry_key=s2.dim_cntry_key
        and s2.end_cal_dim_id='9999-01-01'
      where s2.dim_cntry_key is null
         or (s1.compare_key != s2.compare_key) 
                     ),
{% else %} 
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
			s1.eff_cal_dim_id,
			s1.end_cal_dim_id,
			s1.inst_ts,
			s1.scd_key,
			s1.compare_key,
            null as upd_scd_key,
            null as upd_end_cal_dim_id
       from {{ ref('wrk_mart_world_population') }} s1
                     ),
{% endif %}                                          
wrld_scd2_1 as (                  
select dim_cntry_key,
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
	   compare_key,
       'I' as upd_ins_flg
  from wrld_scd2 
  union all 
select dim_cntry_key,
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
	   upd_end_cal_dim_id as end_cal_dim_id,
	   inst_ts,
	   upd_scd_key as scd_key,
	   compare_key,
       'U' as upd_ins_flg
  from wrld_scd2 
 where not upd_scd_key is null ) 

 select * from wrld_scd2_1 