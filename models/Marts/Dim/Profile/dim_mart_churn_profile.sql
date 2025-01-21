{% if relation_exists( this ) %}
 {{ log("Target table existis so running this block- ", info=true) }}
   select s1.dim_churn_prfl_key,
          s1.tenure,
          s1.numofproducts,
          s1.hascrcard,
          s1.isactivemember,
          s1.exited,
          s1.eff_cal_dim_id,
          s1.inst_ts
     from {{ ref('wrk_mart_churn_profile') }} s1
left join {{this}} s2
       on s1.dim_churn_prfl_key=s2.dim_churn_prfl_key
    where s2.dim_churn_prfl_key is null 
{% else %}
   select dim_churn_prfl_key,
          tenure,
          numofproducts,
          hascrcard,
          isactivemember,
          exited,
          eff_cal_dim_id,
          inst_ts
     from {{ ref('wrk_mart_churn_profile') }} s1
{% endif %}