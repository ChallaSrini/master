{% macro combine_strings(str1, str2) %}
    {{ [str1,"middle", str2] | join('') }}
{% endmacro %}
