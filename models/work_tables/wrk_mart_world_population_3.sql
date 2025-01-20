
{{ config(materialized='table') }}

{%- set source_relation = adapter.get_relation(
      database=this.database,
      schema=this.schema,
      identifier=source("my_src","tgt_population_test").table) -%}

{% if source_relation.table == source("my_src","tgt_population_test").table %}
{{ log("Source Relation: " ~ source_relation , info=true) }}
    select *
      from {{ ref('wrk_mart_world_population') }} s1
{% else %}
{{ log("Source Relation doesn't exist: " ~ source("my_src","tgt_population_test").table, info=true) }}
    select *,'extra_column' as ext_col
      from {{ ref('wrk_mart_world_population') }} s1
{% endif %}




 
