{% macro validate_required_params() %}
  {% if flags.WHICH != 'docs' and flags.WHICH != 'compile' and flags.WHICH != 'test' and flags.WHICH != 'debug'   %}
    {% if var('buss_dt','9999-01-01') == '' %}
      {{ exceptions.raise_compiler_error("Missing required parameter: 'buss_dt'") }}
    {% endif %}
    {% if not var('run_id', default=None) %}
      {{ exceptions.raise_compiler_error("Missing required parameter: 'run_id'") }}
    {% endif %}
  {%else%}
    {% set buss_dt = var('buss_dt', '9999-01-01') %}
    {% set run_id = var('run_id', '9999') %}     
  {% endif %}
{% endmacro %}