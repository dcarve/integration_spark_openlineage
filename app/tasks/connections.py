import asyncio
import logging

import psycopg2
from config.security_service import SecurityService
from io_db.query_generator import DbConfigs
from io_dbms.query_generator import DbmsConfigs
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from stream_config import KafkaStream

logging.basicConfig(level=70)
logger = logging.getLogger('main')


class Connections:
    def __init__(self):
        self.neo4j_conn = False
        self.postgres_conn = False
        self.kafka_conn = False

    async def neo4j(self, con_config=None):
        if con_config:
            host = con_config.host
            port = con_config.port
            usr = con_config.usr
            pwd = con_config.pwd

        else:
            dbms_conn_config = DbmsConfigs()

            host = dbms_conn_config.host
            port = dbms_conn_config.port
            usr = dbms_conn_config.user
            pwd = dbms_conn_config.password

        driver = GraphDatabase.driver(
            f"bolt://{host}:{port}",
            auth=(usr, pwd),
        )

        session = driver.session()
        try:
            nodes = session.run("Match () Return 1 Limit 1")
            self.neo4j_conn = True
        except Exception as e:
            self.neo4j_conn = False
            logging.process_info(f"{e}")

        logging.process_info(f"  neo4j_conn {self.neo4j_conn}")

        await asyncio.sleep(3)

    async def postgres(self, con_config=None):
        if con_config:
            host = con_config.host
            port = con_config.port
            db = con_config.db
            usr = con_config.usr
            pwd = con_config.pwd

        else:
            db_conn_config = DbConfigs()

            host = db_conn_config.host
            port = db_conn_config.port
            db = db_conn_config.database
            usr = db_conn_config.user
            pwd = db_conn_config.password

        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=db,
                user=usr,
                password=pwd,
            )
            conn.close()
            self.postgres_conn = True
        except Exception as e:
            self.postgres_conn = False
            logging.process_info(f"{e}")

        logging.process_info(f"  postgres_conn {self.postgres_conn}")

        await asyncio.sleep(3)

    async def kafka(self, con_config=None):
        if con_config:
            host = con_config.host
            port = con_config.port

        else:
            kafka_connect = KafkaStream()
            host = kafka_connect.KAFKA_HOST
            port = kafka_connect.KAFKA_PORT

        try:
            consumer = KafkaConsumer(group_id='openlineage', bootstrap_servers=(f"{host}:{port}"))
            topics = consumer.topics()

            if not topics:
                self.kafka_conn = False
                logging.process_info(f"  kafka_conn {self.kafka_conn}")
            else:
                self.kafka_conn = True
                logging.process_info(f"  topics: {topics}")
                logging.process_info(f"  kafka_conn {self.kafka_conn}")
        except Exception as e:
            self.kafka_conn = False
            logging.process_info(f"{e}")
            logging.process_info(f"  kafka_conn {self.kafka_conn}")

        await asyncio.sleep(3)
