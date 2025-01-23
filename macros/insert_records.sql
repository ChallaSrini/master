{% macro insert_records(target_table='', src_table='', col_list=[], where_conditions='') %}
-- Macro to perform an INSERT operation for new records
-- Args:
-- target_table (str): Fully qualified target table name.
-- src_table (str): Fully qualified source table name.
-- col_list (list): List of columns to insert (e.g., ["col1", "col2", "col3"]).
-- where_conditions (list): Optional list of WHERE conditions to filter source rows (e.g., ["src.flag = 'I'"]).

-- Generate the column list for INSERT
{% set columns = col_list | join(", ") %}

-- Construct and return the INSERT SQL
insert into {{ target_table }} ({{ columns }})
select {{ columns }}
from {{ src_table }} src
where {{ where_conditions }} in ('I','U','INSERT','UPDATE','Insert','Update')
{% endmacro %}
