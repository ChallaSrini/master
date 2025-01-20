 {{ config(materialized="table") }}

{% if relation_exists(source("my_src", "tgt_population_test")) %}
    {{ log("Source Relation: " ~ source("my_src", "tgt_population_test"), info=true) }}
    select *
    from {{ ref("wrk_mart_world_population") }} s1
{% else %}
    {{
        log(
            "Source Relation doesn't exist: "
            ~ source("my_src", "tgt_population_test").table,
            info=true,
        )
    }}
    select *, 'extra_column' as ext_col
    from {{ ref("wrk_mart_world_population") }} s1
{% endif %}