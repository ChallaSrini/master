{% materialization scd2_custom_materialization, adapter='default' %}
{# Begin materialization #}

--parametrs defining for execution
{% set target_table = this %}
{% set src_table_nm=this.database ~ '.' ~ this.schema ~ '.' ~ this.identifier ~ '_test' %}
{% set tgt_clmn_lst = get_column_names(this.database,this.schema,this.identifier) %}

--parameters passed through model
{% set pk_keys = model.config.get('pk_keys', 'default_value') %}
{% set ins_upd_flag = model.config.get('ins_upd_flag', 'iu_flag') %}



-- Log parameters for debugging
{% do log("src_table_nm: " ~ src_table_nm, info=True) %}
{% do log("tgt_clmn_lst: " ~ tgt_clmn_lst, info=True) %}
{% do log("pk_keys in sourec & target tale: " ~ pk_keys, info=True) %}
{% do log("ins_upd_flag: " ~ ins_upd_flag, info=True) %}


{% do log("context: " ~ context[my_new_project], info=True) %}

{# Run pre-hooks #}
{{ run_hooks(pre_hooks) }}
    

-- Log a message for debugging (optional)
{% do log("Executing scd2 materialization for: " ~ target_table, info=True) %}

{% set compiled_sql = model.compiled_sql %}

-- Step 2: Log the compiled SQL for debugging (optional)
{% do log("Compiled SQL: " ~ compiled_sql, info=True) %}

{% call statement("main") %}
create or replace view {{src_table_nm}} as
{{compiled_sql}}
{% endcall %}

{% do log("Temp view creation completed" , info=True) %}

-- Step 3: Execute the SQL from the model file
{% do run_query(compiled_sql) %}
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- starting of updation operation
{% do log("Preparing the update query for table" ~ target_table, info=True) %}

{% set update_query_scd2= merge_update(target_table= target_table, src_table=src_table_nm, pk_keys=pk_keys, 
 where_conditions=ins_upd_flag) %}

{% do log("Compiled query for updation" ~ update_query_scd2 , info=True) %}

{% do log("Starting update operation on target table" ~ target_table, info=True) %}

{% call statement("main") %}
{{ update_query_scd2 }}
{% endcall %}
{% do log("Update operation completed on target table" ~ target_table, info=True) %}
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- starting of insert operation
{% do log("Preparing the insert query for table" ~ target_table, info=True) %}

{% set insert_query_scd2=insert_records(target_table=target_table, src_table= src_table_nm, col_list= tgt_clmn_lst, where_conditions=ins_upd_flag) %}

{% do log("Compiled query for insertion" ~ insert_query_scd2 , info=True) %}

{% do log("Starting insert operation on target table" ~ target_table, info=True) %}


{% call statement("main") %}
--{{insert_query_scd2}}
{% endcall %}
{% do log("Insert operation completed on target table" ~ target_table, info=True) %}
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- dropping of temp view 

{% do log("Dropping the temp view" ~ src_table_nm, info=True) %}

{% call statement("main") %}
DROP VIEW IF EXISTS {{src_table_nm}}
{% endcall %}

-------------------------------------------------
----------------------------------------------

{{ run_hooks(post_hooks) }}

{% do return({'relations': [target_table]}) %}

{% endmaterialization %}
