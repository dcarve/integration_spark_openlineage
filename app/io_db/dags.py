from io_db import DefaultIo


class Dags(DefaultIo):
    def __init__(self, select_data):
        self.table = "openlineage.dags"

        self.select = f"""
        SELECT 
            tmp.dag AS name,
            tmp.airflow_env AS airflow_env,
            current_timestamp AS create_at,
            current_timestamp AS update_at
        FROM ({select_data}) tmp
        LEFT JOIN openlineage.dags
            ON openlineage.dags.name = tmp.dag
            AND openlineage.dags.airflow_env = tmp.airflow_env
        """

        self.where = """
            openlineage.dags.name isNull
            AND openlineage.dags.airflow_env isNull
        """

    @classmethod
    def convert_pandas_to_select_mock(cls, df):
        mock_select = cls.convert_pandas_to_mock_sql(df, ['dag', 'airflow_env'])
        return cls(select_data=mock_select)

    @classmethod
    def read_from_staging(cls):  # pragma: no cover
        select_staging = """
            SELECT distinct
                dag,
                airflow_env
            FROM staging.datasets
        """
        return cls(select_data=select_staging)

    def insert(self):
        return self.insert_statement(
            self.select,
            self.where,
            columns=["name", "airflow_env", "create_at", "update_at"],
            table_to_insert=self.table,
        )

    def update(self):
        return self.update_statement(
            self.select,
            self.where,
            columns=["name", "airflow_env"],
            table_to_update=self.table,
        )

    def convert_to_query(self):
        return self.prettier_sql(self.insert() + ';\n\n' + self.update() + ';\n\n')
