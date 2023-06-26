import logging
import os
import sqlite3
from typing import List, Sequence

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=70)
logger = logging.getLogger('tasks.read_stream_kafka.py')


SAVE_LOGS_FOR_DEGUB = False


def create_conn():  # pragma: no cover
    try:
        conn = sqlite3.connect('config/db_configs.db')
    except Exception as e:
        try:
            conn = sqlite3.connect('app/config/db_configs.db')
        except Exception as e:
            raise e('sqlite file not found')

    return conn


def create_db_envs():  # pragma: no cover
    conn = create_conn()

    list_tables = pd.read_sql(
        """SELECT name FROM sqlite_master WHERE type='table' and name='connections';""", conn
    )
    if list_tables.shape[0] == 0:
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS connections(
                type_name text,
                host text,
                port text,
                db text,
                usr text,
                pwd text,
                topic text
            )"""
        )
        conn.commit()
        cur.execute(
            f"""INSERT INTO connections (
                type_name,
                host,
                port,
                db,
                usr,
                pwd,
                topic
            )
            VALUES (
                'postgres',
                '{os.getenv("POSTGRES_HOST")}',
                '{os.getenv("POSTGRES_PORT")}',
                '{os.getenv("POSTGRES_DATABASE")}',
                '{os.getenv("POSTGRES_USER")}',
                '{os.getenv("POSTGRES_PASSWORD")}',
                null
            )"""
        )
        cur.execute(
            f"""INSERT INTO connections (
                type_name,
                host,
                port,
                db,
                usr,
                pwd,
                topic
            ) 
            VALUES (
                'kafka',
                '{os.getenv("KAFKA_HOST")}',
                '{os.getenv("KAFKA_PORT")}',
                null,
                null,
                null,
                '{os.getenv("KAFKA_TOPIC")}'
            )"""
        )

        cur.execute(
            f"""INSERT INTO connections (
                type_name,
                host,
                port,
                db,
                usr,
                pwd,
                topic
            ) 
            VALUES (
                'neo4j',
                '{os.getenv("NEO4J_HOST")}',
                '{os.getenv("NEO4J_PORT")}',
                null,
                '{os.getenv("NEO4J_USR")}',
                '{os.getenv("NEO4J_PWD")}',
                null
            )"""
        )
        conn.commit()
        conn.close()


def get_conn_config(type_name, type_conf):  # pragma: no cover
    conn = create_conn()
    res = pd.read_sql(
        f"""SELECT {type_conf} FROM connections WHERE type_name='{type_name}'""", conn
    )
    conn.close()
    return res[type_conf].to_list()[0]


def update_conn_config(type_name, type_conf, value):  # pragma: no cover
    conn = create_conn()
    cur = conn.cursor()

    query = f"""
        UPDATE connections
        SET {type_conf} = '{value}'
        WHERE
            type_name = '{type_name}'
        """

    cur.execute(query)
    conn.commit()
    conn.close()


def update_conn_config_params(params: List[Sequence]):  # pragma: no cover
    for param in params:
        update_conn_config(type_name=param[0], type_conf=param[1], value=param[2])
