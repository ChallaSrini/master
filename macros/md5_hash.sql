{% macro md5_hash(columns) %}
    upper(MD5(CONCAT_WS(',', {{ ','.join(columns) }})))
{% endmacro %}