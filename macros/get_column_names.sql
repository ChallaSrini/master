{% macro get_column_names(database, schema, table_name) %}
    {% set relation = adapter.get_relation(database=database, schema=schema, identifier=table_name) %}
    {% if relation is not none %}
        {% set columns = adapter.get_columns_in_relation(relation) %}
        {{ return(columns | map(attribute='name') | list) }}
    {% else %}
        {{ return([]) }}  {# Return an empty list if the relation does not exist #}
    {% endif %}
{% endmacro %}