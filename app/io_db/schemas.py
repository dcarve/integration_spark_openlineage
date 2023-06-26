from io_db import DefaultIo


class Schemas(DefaultIo):
    def __init__(self, select_data):
        self.table = "openlineage.schemas"

        self.select = f"""
        select 
            tmp.dataset_schema_json as schema_json,
            openlineage.datasets.id as dataset_id,
            current_timestamp as create_at,
            current_timestamp as update_at
        FROM ({select_data}) tmp
        left join openlineage.datasets
            on openlineage.datasets.name = tmp.dataset_name
            and openlineage.datasets.uri = tmp.dataset_uri
            and openlineage.datasets.location = tmp.dataset_location
            and openlineage.datasets.namespace = tmp.dataset_namespace

        left join openlineage.schemas
            on openlineage.schemas.dataset_id = openlineage.datasets.id
            and openlineage.schemas.schema_json = tmp.dataset_schema_json
        """

        self.where = """
            openlineage.schemas.dataset_id isNull
            and openlineage.schemas.schema_json isNull
        """

    @classmethod
    def convert_pandas_to_select_mock(cls, df):
        mock_select = cls.convert_pandas_to_mock_sql(
            df,
            [
                'dataset_location',
                'dataset_uri',
                'dataset_name',
                'dataset_namespace',
                'dataset_schema_json',
            ],
        )
        return cls(select_data=mock_select)

    @classmethod
    def read_from_staging(cls):  # pragma: no cover
        select_staging = """
        select distinct
            dataset_location,
            dataset_uri,
            dataset_name,
            dataset_namespace,
            dataset_schema_json
        from staging.datasets
        """
        return cls(select_data=select_staging)

    def insert(self):
        return self.insert_statement(
            self.select,
            self.where,
            columns=["schema_json", "dataset_id", "create_at", "update_at"],
            table_to_insert=self.table,
        )

    def update(self):
        return self.update_statement(
            self.select,
            self.where,
            columns=["schema_json", "dataset_id"],
            table_to_update=self.table,
        )

    def convert_to_query(self):
        return self.prettier_sql(self.insert() + ';\n\n' + self.update() + ';\n\n')
