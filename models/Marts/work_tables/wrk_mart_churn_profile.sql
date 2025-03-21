select distinct
    {{ md5_hash(['tenure','numofproducts','hascrcard','isactivemember','exited']) }} as dim_churn_prfl_key,
    tenure,
    numofproducts,
    hascrcard,
    isactivemember,
    exited,
    cast('{{ var("buss_dt",'9999-01-01') }}' as date) as eff_cal_dim_id,
    current_timestamp() as inst_ts
from {{ source("my_src","src_prfl_churn") }}
