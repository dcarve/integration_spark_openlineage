import logging

import pandas as pd
import psycopg2
from config.env_var import get_conn_config
from io_db.dags import Dags
from io_db.datasets import Datasets
from io_db.fields import Fields
from io_db.lineage_fields import LineageFields
from io_db.runs import Runs
from io_db.schemas import Schemas
from io_db.tasks import Tasks

logging.basicConfig(level=70)
logger = logging.getLogger('io_db.query_generator')


class DbConfigs:
    def __init__(self):  # pragma: no cover
        self.host = get_conn_config('postgres', 'host')
        self.port = get_conn_config('postgres', 'port')
        self.database = get_conn_config('postgres', 'db')
        self.user = get_conn_config('postgres', 'usr')
        self.password = get_conn_config('postgres', 'pwd')

        logging.process_info(
            f"  postgres host: {self.host}:{self.port}"
            f", database: {self.database}"
            f", user: {self.user}"
            f", password: {self.password[0]+'*'*len(self.password[1:])}"
        )


def create_sql_query(df_datasets, df_lineage, df_runs):
    queries = None

    if isinstance(df_datasets, pd.core.frame.DataFrame):
        queries = [
            Dags.convert_pandas_to_select_mock(df_datasets).convert_to_query(),
            Tasks.convert_pandas_to_select_mock(df_datasets).convert_to_query(),
            Datasets.convert_pandas_to_select_mock(df_datasets).convert_to_query(),
            Schemas.convert_pandas_to_select_mock(df_datasets).convert_to_query(),
            Fields.convert_pandas_to_select_mock(df_datasets).convert_to_query(),
        ]

    if isinstance(df_runs, pd.core.frame.DataFrame):
        queries = queries + [Runs.convert_pandas_to_select_mock(df_runs).convert_to_query()]

    if isinstance(df_lineage, pd.core.frame.DataFrame):
        queries = queries + [
            LineageFields.convert_pandas_to_select_mock(df_lineage).convert_to_query()
        ]

    logging.process_info("  Logs converted to sql statement")

    return queries


def send_data_db(db_conn_config, list_queries):  # pragma: no cover
    logging.process_info("  send to postgres")

    conn = psycopg2.connect(
        host=db_conn_config.host,
        port=db_conn_config.port,
        database=db_conn_config.database,
        user=db_conn_config.user,
        password=db_conn_config.password,
    )
    cur = conn.cursor()
    for query in list_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e, query)
            raise

    conn.commit()
    cur.close()
    conn.close()
