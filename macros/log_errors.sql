{% macro log_errors(results) %}
    {{ log("========== Begin log errors ==========", info=True) }}
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
        {{ log("Insert rows" ~ ins_rows)}}
        {% if ins_rows %}
        -- Build the CTE
            {% set ins_cte_query = "WITH results_cte AS (\n  SELECT * FROM (VALUES\n    " ~ ins_rows | join(',\n    ') ~ "\n  ) AS t(invocation_id, unique_id, status,message)\n) " %}
		
		    {% set error_query= "INSERT INTO " ~ env_var('DBT_AUDIT_SCHEMA') ~ ".dbt_error_dtl \n" ~ins_cte_query~ "SELECT invocation_id, unique_id, message, current_timestamp() FROM results_cte " %}
		
		    {{ log("Generated Error Insertion Query:\n" ~ error_query, info=True) }}
            {{ return(error_query) }}
        {% else %}
            {{ return("-- No execution occurred (dry run).") }}
    {% else %}
        {{ return("-- No execution occurred (dry run).") }}
    {% endif %}

    {{ log("========== End log errors ==========", info=True) }}
{% endmacro %}
