{% macro log_status(results) %}
    {{ log("========== Begin log status ==========", info=True) }}
    {% if execute %}
        -- Get the invocation_id
        {% set invocation_id = invocation_id %}

        -- Create a list to store rows for the CTE
        {% set rows = [] %}
		{% set ins_rows = [] %}

        {% for res in results -%}
			{%- if res.status == 'error' %}
				{% set line = "node: " ~ res.node.unique_id ~ "; status: " ~ res.status %}
				{{ log(line, info=True) }}

				{% do ins_rows.append("('" ~ invocation_id ~ "', '" ~ res.node.unique_id.split('.')[-1] ~ "', '" ~ res.status ~ "', '"~ res.message[:500] ~"' )") %}
				{% do rows.append("('" ~ invocation_id ~ "', '" ~ res.node.unique_id.split('.')[-1] ~ "', '" ~ res.status ~ "', '"~ res.message[:500] ~"' )") %}
			{% else %}
                {% do rows.append("('" ~ invocation_id ~ "', '" ~ res.node.unique_id.split('.')[-1] ~ "', '" ~ res.status ~ "', '"~ res.message[:500] ~"' )") %}
			{%- endif %}
				
        {%- endfor %}


        {% set upd_query = "WITH results_cte AS (\n  SELECT * FROM (VALUES\n    " ~ rows | join(',\n    ') ~ "\n  ) AS t(invocation_id, unique_id, status,message)\n)
            merge into  {{ env_var('DBT_AUDIT_SCHEMA') }}.dbt_run_dtl_info tgt
                 using results_cte src
                    on tgt.invocation_id = src.invocation_id 
                   and tgt.component_name = src.unique_id
                  when matched and tgt.end_time is null
                  then update 
                   set tgt.status =src.status,tgt.end_time = current_timestamp()   " %}
		
		{{ log("Generated Merge Query:\n" ~ upd_query, info=True) }}
        {{ return(upd_query) }}
    {% else %}
        {{ return("-- No execution occurred (dry run).") }}
    {% endif %}
    {{ log("========== End log status ==========", info=True) }}
{% endmacro %}
