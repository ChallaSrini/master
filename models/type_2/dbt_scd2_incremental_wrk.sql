{{ config(
    materialized='incremental',
    pre_hook="Truncate table {{this}}"
 ) }}


select  md5(concat_ws('$',1,'NY')) as emp_id,1 as id,'kane williamson' as name,25 as age,'NY' as address,'Y' as is_active ,md5(concat_ws('$',name,age,is_active)) as md5_key union all
select  md5(concat_ws('$',7,'CA')) as emp_id,7 as id,'steve Smith' as name,37 as age,'LA' as address,'N' as is_active,md5(concat_ws('$',name,age,is_active))  as md5_key
