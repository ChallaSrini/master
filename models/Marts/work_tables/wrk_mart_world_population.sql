with wrld_popln as (
    select {{ md5_hash(['country']) }} as dim_cntry_key,
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
           cast('{{ var("buss_dt",'9999-01-01') }}' as date) as eff_cal_dim_id,
           cast('9999-01-01' as date) as end_cal_dim_id,
           CURRENT_TIMESTAMP() as inst_ts,
           {{ md5_hash(['country','eff_cal_dim_id'])}} as scd_key,
           {{ md5_hash(['population','yearly_change','net_change','density','land_area','migrants','fert_rate','med_age','urban_pop','world_share'])}} as compare_key
      from {{ source("my_src","src_population")}}
)

select * from wrld_popln
