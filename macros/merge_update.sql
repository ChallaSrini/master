{% macro merge_update(target_table='', src_table='', pk_keys={}, where_conditions='') %}

-- Generate the ON conditions based on primary key mappings
{% set on_conditions = [] %}
{% for tgt_key, src_key in zip(pk_keys.tgt_pk_keys, pk_keys.src_pk_keys) %}
    {% do on_conditions.append("tgt." ~ tgt_key ~ " =  src." ~ src_key) %}
{% endfor %}
{% set on_clause = on_conditions | join(" AND ") %}

-- Construct and return the MERGE SQL
merge into {{ target_table }} tgt
using {{ src_table }} src
    on {{ on_clause }}
    AND tgt.end_cal_dim_id = '9999-01-01'
when matched
    AND {{ where_conditions }} in ('U','UPDATE','Update')
    then update
    set tgt.end_cal_dim_id = date_add(src.eff_cal_dim_id,-1)
{% endmacro %}