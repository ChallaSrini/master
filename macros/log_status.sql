{% macro log_status() %}
    {{ log("Flag informaiton for validation" ~ flags, info=True)}}
    {{ log("Flag informaiton for validation" ~ flags.WHICH, info=True)}}
    {% if flags.WHICH !='run' and flags.WHICH !='snapshots' %}
        {% set status_query= "select 'dummy'" %}
        {{ return(status_query) }}
    {% else %} 
    {% if execute %}
        {{ log("Execution mode after DBT build: TRUE", info=True) }}
		{% set status_rows = [] %}
		{% for res in results -%}
            {% do status_rows.append("('" ~ invocation_id ~ "', '" ~ res.node.unique_id.split('.')[-1] ~ "', '" ~ res.status ~ "', '"~ res.message[:500]| replace("'", "") ~"' )") %}
		{%- endfor %}
		{% if status_rows %}
			        {% set status_query = "WITH results_cte AS (\n  SELECT * FROM (VALUES\n    " ~ status_rows | join(',\n    ') ~ "\n  ) AS t(invocation_id, unique_id, status,message)\n)
            merge into "  ~ env_var('DBT_AUDIT_SCHEMA') ~".dbt_run_dtl_info tgt
                 using results_cte src
                    on tgt.invocation_id = src.invocation_id 
                   and tgt.component_name = src.unique_id
                  when matched and tgt.end_time is null
                  then update 
                   set tgt.status =src.status,tgt.end_time = current_timestamp()   " %}
		{% else %}
			{% set status_query= "select 'dummy'" %}
		{% endif %}		
    {% else %}
        {{ log("Execution mode after DBT build: FALSE", info=True) }}
		{% set status_query= "select 'dummy'" %}
    {% endif %}
  {#  {{ log("Results metadata: " ~ results, info=True) }} #}
    {{ log("Results audit- Completed") }}
    {{ return(status_query) }}
    {% endif %}
{% endmacro %}




