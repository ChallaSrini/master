 {{ config(materialized="table") }}

{% if relation_exists(source("my_src", "tgt_population_test")) %}
    {{ log("Source Relation: " ~ source("my_src", "tgt_population_test"), info=true) }}
    select * from {{ ref('world_population_check') }}
{% else %}
    {{
        log(
            "Source Relation doesn't exist: "
            ~ source("my_src", "tgt_population_test").table,
            info=true,
        )
    }}
    select * from {{ ref('world_population_check') }} s1
{% endif %}