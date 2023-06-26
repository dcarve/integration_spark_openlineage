import asyncio
import logging
import time

from config.env_var import create_db_envs, update_conn_config_params
from etl import ParseMsg
from io_db.query_generator import DbConfigs, create_sql_query, send_data_db
from io_dbms.query_generator import (
    DbmsConfigs,
    create_cypher_query,
    send_data_neo4j,
)
from stream_config import KafkaStream

logging.basicConfig(level=70)
logger = logging.getLogger('tasks.read_stream_kafka.py')


class ReadStream:
    def __init__(self):
        self.running = False
        self.neo4j_load = False
        self.change_conn = None

    async def get_configs(self):
        create_db_envs()

        logging.process_info("  Create Kafka Consumer Connection")
        self.kafka_connect = KafkaStream()
        self.db_conn_config = DbConfigs()
        self.dbms_conn_config = DbmsConfigs()

        await asyncio.sleep(1)

    async def change_configs(self, dict_json):
        if dict_json.type_conn == 'postgres':
            logging.process_info(f"  Alterando configurações do {dict_json.type_conn}")

            try:
                update_conn_config_params(
                    [
                        ('postgres', 'host', dict_json.host),
                        ('postgres', 'port', dict_json.port),
                        ('postgres', 'db', dict_json.db),
                        ('postgres', 'usr', dict_json.usr),
                        ('postgres', 'pwd', dict_json.pwd),
                    ]
                )

                self.db_conn_config = DbConfigs()
                self.change_conn_res = None
            except Exception as e:
                self.change_conn = str(e)

        elif dict_json.type_conn == 'kafka':
            logging.process_info(f"  Alterando configurações do {dict_json.type_conn}")

            try:
                update_conn_config_params(
                    [
                        ('kafka', 'host', dict_json.host),
                        ('kafka', 'port', dict_json.port),
                        ('kafka', 'topic', dict_json.topic),
                    ]
                )

                self.kafka_connect = KafkaStream()
                self.change_conn_res = None
            except Exception as e:
                self.change_conn = str(e)

        elif dict_json.type_conn == 'neo4j':
            logging.process_info(f"  Alterando configurações do {dict_json.type_conn}")

            try:
                update_conn_config_params(
                    [
                        ('neo4j', 'host', dict_json.host),
                        ('neo4j', 'port', dict_json.port),
                        ('neo4j', 'usr', dict_json.usr),
                        ('neo4j', 'pwd', dict_json.pwd),
                    ]
                )

                self.dbms_conn_config = DbmsConfigs()
                self.change_conn_res = None
            except Exception as e:
                self.change_conn = str(e)
        else:
            self.change_conn = 'connection type is not accepted'

        await asyncio.sleep(1)

    async def start(self):
        self.running = True

        while self.running:
            consumer = self.kafka_connect.open_topic()
            count_msg = 0

            logging.process_info("  Search for messages")

            for msg in consumer:
                df_datasets, df_lineage, df_runs = ParseMsg(msg).run()
                queries_sql = create_sql_query(df_datasets, df_lineage, df_runs)
                if queries_sql:
                    send_data_db(self.db_conn_config, queries_sql)
                count_msg += 1

            logging.process_info(f"  {count_msg} messages readed and send to DB")

            if self.neo4j_load:
                queries = create_cypher_query()
                logging.process_info("  Send data do Neo4j")
                send_data_neo4j(self.dbms_conn_config, queries)

            logging.process_info("  Commit kafka Consumer/Close Comsumer")

            consumer.close(autocommit=True)
            time.sleep(10)
            await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        logging.process_info("  Kafka read stream stopped")
        await asyncio.sleep(1)

    async def turn_on_neo4j_load(self):
        self.neo4j_load = True
        logging.process_info("  Neo4j load restart")
        await asyncio.sleep(1)

    async def turn_off_neo4j_load(self):
        self.neo4j_load = False
        logging.process_info("  Neo4j load stopped")
        await asyncio.sleep(1)
