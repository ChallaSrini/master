{% macro relation_exists(relation_ref) %}
    
{%- set relation = adapter.get_relation(
      database=relation_ref.database,
      schema=relation_ref.schema,
      identifier=relation_ref.table) -%}
    
    {% if relation is none %}
        {{ return(false) }}
    {% else %}
        {{ return(true) }}
        {{ log("Table exists: " ~ relation_ref, info=true) }}
    {% endif %}
 
{% endmacro %}