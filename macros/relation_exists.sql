{% macro relation_exists(relation_ref) %}
    
{%- set relation = adapter.get_relation(
      database=relation_ref.database,
      schema=relation_ref.schema,
      identifier=relation_ref.table) -%}
    {{ log("Checking if relation exists or not: ", info=true) }}
    {% if relation is none %}
        {{ return(false) }}
    {% else %}
        {{ return(true) }}
    {% endif %}
 
{% endmacro %}