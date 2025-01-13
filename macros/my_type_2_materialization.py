from dbt.adapters.base import AdapterPlugin
from dbt.adapters.base.relation import Relation
from dbt.contracts.results import RunResult
from dbt.exceptions import RuntimeException
from dbt.events import AdapterLogger

logger = AdapterLogger("my_type_2_materialization")


class MyType2MaterializationPlugin(AdapterPlugin):
    def __init__(self, config):
        super().__init__(config)

    def create_model(self, model, relations):
        """
        This method is called by dbt to create the model.

        Args:
            model: The dbt Model object.
            relations: A dictionary of relations.

        Returns:
            RunResult: A RunResult object indicating the success or failure of the operation.
        """
        if model.config.get('materialized') != 'type_2':
            return RunResult(status='skipped')

        target_relation = self.get_relation(model, relations)

        # Get source relation
        source_relation = self.get_relation(model.config.get('source'), relations)

        # Get effective_from and effective_to columns
        effective_from_col = model.config.get('effective_from_column', 'effective_from')
        effective_to_col = model.config.get('effective_to_column', 'effective_to')

        # Get load_datetime column
        load_datetime_col = model.config.get('load_datetime_column', 'load_datetime')

        # Construct SQL for Type 2 SCD
        sql = f"""
        MERGE INTO {target_relation} t
        USING (
            SELECT 
                *,
                {load_datetime_col} AS {load_datetime_col} 
            FROM {source_relation}
        ) s
        ON t.{model.unique_key} = s.{model.unique_key}
        WHEN MATCHED AND t.{effective_to_col} IS NULL AND s.{load_datetime_col} > t.{load_datetime_col} 
            THEN UPDATE SET 
                {effective_to_col} = CURRENT_TIMESTAMP(),
                {load_datetime_col} = s.{load_datetime_col}
        WHEN NOT MATCHED BY TARGET 
            THEN INSERT (
                {', '.join([c.name for c in model.columns])}, 
                {effective_from_col},
                {effective_to_col},
                {load_datetime_col}
            ) VALUES (
                {', '.join([f's.{c.name}' for c in model.columns])}, 
                CURRENT_TIMESTAMP(),
                NULL,
                s.{load_datetime_col}
            )
        """

        self.execute(sql)

        return RunResult(status='success')

    # ... other methods (e.g., get_relation, execute) ...

# Register the plugin
Plugin = MyType2MaterializationPlugin