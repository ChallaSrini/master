{% macro log_errors() %}
    {{ log("Flag informaiton for validation" ~ flags, info=True)}}
    {{ log("Flag informaiton for validation" ~ flags.WHICH, info=True)}}
    {% if flags.WHICH !='run' and flags.WHICH !='snapshots' %}
        {% set error_query= "select 'dummy'" %}
        {{ return(error_query) }}
    {% else %} 
    {% if execute %}
        {{ log("Execution mode after DBT run: TRUE", info=True) }}
		{% set error_rows = [] %}
		{% for res in results -%}
			{%- if res.status == 'error' %}
				{% do error_rows.append("('" ~ invocation_id ~ "', '" ~ res.node.unique_id.split('.')[-1] ~ "', '" ~ res.status ~ "', '"~ res.message[:500]| replace("'", "") ~"' )") %}
			{%- endif %}
		{%- endfor %}
		{% if error_rows %}
			{% set error_cte_query = "WITH results_cte AS (\n  SELECT * FROM (VALUES\n    " ~ error_rows | join(',\n    ') ~ "\n  ) AS t(invocation_id, unique_id, status,message)\n) " %}
			{% set error_query= "INSERT INTO " ~ env_var('DBT_AUDIT_SCHEMA') ~ ".dbt_error_dtl \n" ~error_cte_query~ "SELECT invocation_id, unique_id, message, current_timestamp() FROM results_cte; " %}
		{% else %}
			{% set error_query= " select 'dummy'" %}
		{% endif %}
    {% else %}
        {{ log("Execution mode after DBT run: FALSE", info=True) }}
		{% set error_query= "select 'dummy'" %}
    {% endif %}
{#    {{ log("Results metadata: " ~ results, info=True) }} #}
    {{ log("Results audit- Completed") }}
    {{ return(error_query) }}
    {% endif %} 
{% endmacro %}




