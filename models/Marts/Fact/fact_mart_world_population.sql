{% if relation_exists( this ) %}
{{ config(
    pre_hook= "delete from {{ this }} where batch_dt=cast('{{ var('buss_dt','9999-01-01') }}' as date) "
) }}
{% else %}
{{ config(
    materialized="incremental"
) }}
{% endif %}
{{ log("Checking if relation exists or not: ", info=true) }}
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
       eff_cal_dim_id as batch_dt,
       inst_ts
FROM {{ ref('wrk_mart_world_population') }}
