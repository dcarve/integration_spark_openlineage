from io_db import DefaultIo


class Tasks(DefaultIo):
    def __init__(self, select_data):
        self.table = "openlineage.tasks"

        self.select = f"""
        select 
            tmp.task as name,
            openlineage.dags.id as dag_id,
            current_timestamp as create_at,
            current_timestamp as update_at
        FROM ({select_data}) tmp
        left join openlineage.dags
            on openlineage.dags.name = tmp.dag
            and openlineage.dags.airflow_env = tmp.airflow_env
        
        left join openlineage.tasks
            on openlineage.tasks.dag_id = openlineage.dags.id
            and openlineage.tasks.name = tmp.task
        """

        self.where = """
            openlineage.tasks.dag_id isNull
            and openlineage.tasks.name isNull
        """

    @classmethod
    def convert_pandas_to_select_mock(cls, df):
        mock_select = cls.convert_pandas_to_mock_sql(df, ['dag', 'airflow_env', 'task'])
        return cls(select_data=mock_select)

    @classmethod
    def read_from_staging(cls):  # pragma: no cover
        select_staging = """
            select distinct
                dag,
                airflow_env,
                task
            from staging.datasets
        """
        return cls(select_data=select_staging)

    def insert(self):
        return self.insert_statement(
            self.select,
            self.where,
            columns=["name", "dag_id", "create_at", "update_at"],
            table_to_insert=self.table,
        )

    def update(self):
        return self.update_statement(
            self.select,
            self.where,
            columns=["name", "dag_id"],
            table_to_update=self.table,
        )

    def convert_to_query(self):
        return self.prettier_sql(self.insert() + ';\n\n' + self.update() + ';\n\n')
