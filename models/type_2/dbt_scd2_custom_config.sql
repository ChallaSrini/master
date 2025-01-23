    
{{ config(
    materialized='scd2_custom_materialization',
    pk_keys={'tgt_pk_keys':['emp_id'],'src_pk_keys':['emp_id']},
    ins_upd_flag="iu_flag"
) }}

 
with cte as (
select 
src.emp_id,
src.id,
src.name,
src.age,
src.address,
src.is_active,
src.md5_key,
case when tgt.emp_id is null then 'I'
     when src.emp_id = tgt.emp_id and src.md5_key <> tgt.md5_key then 'U'
     else 'N'
end as IU_Flag
from {{ ref('dbt_scd2_incremental_wrk') }} src
left join {{ this }} tgt
on src.emp_id = tgt. emp_id
and tgt.end_cal_dim_id = '9999-12-31'
)
 
select 
emp_id,
id,
name,
age,
address,
is_active,
12345 as run_id,
current_date() as last_update_date,
md5_key,
current_date() as eff_cal_dim_id,
'9999-12-31' as end_cal_dim_id,
IU_Flag
from cte
;

