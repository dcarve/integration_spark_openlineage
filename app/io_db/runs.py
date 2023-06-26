from io_db import DefaultIo


class Runs(DefaultIo):
    def __init__(self, select_data):
        self.table = "openlineage.runs"

        self.select = f"""
        SELECT
            tmp.spark_logical_plan as spark_logical_plan,
            openlineage.tasks.id  as task_id,
            TO_TIMESTAMP(tmp.event_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS') as event_time, 
            tmp.run_uuid as run_uuid,
            tmp.action_type as action_type,
            datasets_input.id as input_dataset_id,
            datasets_output.id as output_dataset_id,
            schemas_input.id  as input_schema_id,
            schemas_output.id as output_schema_id,
            current_timestamp as create_at,
            current_timestamp as update_at
        FROM ({select_data}) tmp
        left join openlineage.dags
            on openlineage.dags.name = tmp.dag
            and openlineage.dags.airflow_env = tmp.airflow_env

        left join openlineage.tasks
            on openlineage.tasks.name = tmp.task
            and openlineage.tasks.dag_id = openlineage.dags.id

        left join openlineage.datasets as datasets_input
            on datasets_input.name = tmp.dataset_name_input
            and datasets_input.uri = tmp.dataset_uri_input
            and datasets_input.location = tmp.dataset_location_input
            and datasets_input.namespace = tmp.dataset_namespace_input

        left join openlineage.datasets as datasets_output
            on datasets_output.name = tmp.dataset_name_output
            and datasets_output.uri = tmp.dataset_uri_output
            and datasets_output.location = tmp.dataset_location_output
            and datasets_output.namespace = tmp.dataset_namespace_output

        left join openlineage.schemas as schemas_input
            on schemas_input.dataset_id = datasets_input.id
            and schemas_input.schema_json = tmp.dataset_schema_json_input

        left join openlineage.schemas as schemas_output
            on schemas_output.dataset_id = datasets_output.id
            and schemas_output.schema_json = tmp.dataset_schema_json_output

        left join openlineage.runs
            on openlineage.runs.spark_logical_plan = tmp.spark_logical_plan
            and openlineage.runs.event_time = TO_TIMESTAMP(tmp.event_time, 'YYYY-MM-DD"T"HH24:MI:SS.MS')
            and openlineage.runs.run_uuid = tmp.run_uuid
            and openlineage.runs.action_type = tmp.action_type
            and openlineage.runs.task_id = openlineage.tasks.id
            and openlineage.runs.input_dataset_id = datasets_input.id
            and openlineage.runs.output_dataset_id = datasets_output.id
            and openlineage.runs.input_schema_id = schemas_input.id
            and openlineage.runs.output_schema_id = schemas_output.id
        """

        self.where = """
            openlineage.runs.spark_logical_plan isNull
            and openlineage.runs.task_id isNull
            and openlineage.runs.event_time isNull
            and openlineage.runs.run_uuid isNull
            and openlineage.runs.action_type isNull
            and openlineage.runs.input_dataset_id isNull
            and openlineage.runs.output_dataset_id isNull
            and openlineage.runs.input_schema_id isNull
            and openlineage.runs.output_schema_id isNull
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
                'dataset_location_input',
                'dataset_uri_input',
                'dataset_name_input',
                'dataset_namespace_input',
                'dataset_schema_json_input',
                'dataset_location_output',
                'dataset_uri_output',
                'dataset_name_output',
                'dataset_namespace_output',
                'dataset_schema_json_output',
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

            dataset_location_input,
            dataset_uri_input,
            dataset_name_input,
            dataset_namespace_input,
            dataset_schema_json_input, 

            dataset_location_output,
            dataset_uri_output,
            dataset_name_output,
            dataset_namespace_output,
            dataset_schema_json_output
        from staging.runs
        """
        return cls(select_data=select_staging)

    def insert(self):
        return self.insert_statement(
            self.select,
            self.where,
            columns=[
                "spark_logical_plan",
                "task_id",
                "event_time",
                "run_uuid",
                "action_type",
                "input_dataset_id",
                "output_dataset_id",
                "input_schema_id",
                "output_schema_id",
                "create_at",
                "update_at",
            ],
            table_to_insert=self.table,
        )

    def update(self):
        return self.update_statement(
            self.select,
            self.where,
            columns=[
                "spark_logical_plan",
                "task_id",
                "event_time",
                "run_uuid",
                "action_type",
                "input_dataset_id",
                "output_dataset_id",
                "input_schema_id",
                "output_schema_id",
            ],
            table_to_update=self.table,
        )

    def convert_to_query(self):
        return self.prettier_sql(self.insert() + ';\n\n' + self.update() + ';\n\n')
