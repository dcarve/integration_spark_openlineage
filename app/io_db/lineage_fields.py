from io_db import DefaultIo


class LineageFields(DefaultIo):
    def __init__(self, select_data):
        self.table = "openlineage.lineage_fields"

        self.select = f"""
            select 
                openlineage.runs.id as run_id,
                fields_input.id as field_input, 
                fields_output.id as field_output, 
                current_timestamp as create_at,
                current_timestamp as update_at
            FROM ({select_data}) tmp
            left join openlineage.datasets as datasets_output
                on datasets_output.name = tmp.dataset_name
                and datasets_output.uri = tmp.dataset_uri
                and datasets_output.location = tmp.dataset_location
                and datasets_output.namespace = tmp.dataset_namespace

            left join openlineage.schemas as schemas_output
                on schemas_output.dataset_id = datasets_output.id
                and schemas_output.schema_json = tmp.dataset_schema_json

            left join openlineage.fields as fields_output
                on fields_output.schema_id = schemas_output.id
                and fields_output.name = tmp.field_name_output

            left join openlineage.dags
                on openlineage.dags.name = tmp.dag
                and openlineage.dags.airflow_env = tmp.airflow_env

            left join openlineage.tasks
                on openlineage.tasks.name = tmp.task
                and openlineage.tasks.dag_id = openlineage.dags.id

            left join openlineage.runs
                on openlineage.runs.event_time = TO_TIMESTAMP(tmp.event_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS')
                and openlineage.runs.action_type = tmp.action_type
                and openlineage.runs.run_uuid = tmp.run_uuid
                and openlineage.runs.spark_logical_plan = tmp.spark_logical_plan

            inner join openlineage.datasets as datasets_input
                on datasets_input.id = openlineage.runs.input_dataset_id
                and datasets_input.name = tmp.dataset_name_origin
                and datasets_input.namespace = tmp.namespace_origin

            left join openlineage.schemas as schemas_input
                on schemas_input.dataset_id = datasets_input.id
                and schemas_input.id = openlineage.runs.input_schema_id

            left join openlineage.fields as fields_input
                on fields_input.schema_id = schemas_input.id
                and fields_input.name = tmp.field_name_origin

            left join openlineage.lineage_fields
                on openlineage.lineage_fields.run_id = openlineage.runs.id
                and openlineage.lineage_fields.field_input = fields_input.id
                and openlineage.lineage_fields.field_output = fields_output.id
        """

        self.where = """
            openlineage.lineage_fields.run_id isNull
            and openlineage.lineage_fields.field_input  isNull
            and openlineage.lineage_fields.field_output  isNull
        """

    @classmethod
    def convert_pandas_to_select_mock(cls, df):
        mock_select = cls.convert_pandas_to_mock_sql(
            df,
            [
                'event_time',
                'airflow_env',
                'dag',
                'task',
                'action_type',
                'run_uuid',
                'spark_logical_plan',
                'dataset_name_origin',
                'namespace_origin',
                'dataset_location',
                'dataset_uri',
                'dataset_name',
                'dataset_namespace',
                'dataset_schema_json',
                'field_name_output',
                'field_name_origin',
            ],
        )
        return cls(select_data=mock_select)

    @classmethod
    def read_from_staging(cls):  # pragma: no cover
        select_staging = """
            select distinct
                event_time,
                airflow_env,
                dag,
                task,
                action_type,
                run_uuid,
                spark_logical_plan,

                dataset_name_origin,
                namespace_origin,
                dataset_location,
                dataset_uri,
                dataset_name,
                dataset_namespace,
                dataset_schema_json,
                field_name_output,
                field_name_origin

            from staging.lineage
        """
        return cls(select_data=select_staging)

    def insert(self):
        return self.insert_statement(
            self.select,
            self.where,
            columns=["run_id", "field_input", "field_output", "create_at", "update_at"],
            table_to_insert=self.table,
        )

    def update(self):
        return self.update_statement(
            self.select,
            self.where,
            columns=["run_id", "field_input", "field_output"],
            table_to_update=self.table,
        )

    def convert_to_query(self):
        return self.prettier_sql(self.insert() + ';\n\n' + self.update() + ';\n\n')
