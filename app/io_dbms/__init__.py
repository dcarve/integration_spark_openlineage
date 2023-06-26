import logging
import os

import pandas as pd
from config.env_var import get_conn_config
from neo4j import GraphDatabase
from sqlalchemy import create_engine

logging.basicConfig(level=70)
logger = logging.getLogger('main')


class DefaultIo:
    @staticmethod
    def get_last_id_neo4j(label: str):
        host = get_conn_config('neo4j', 'host')
        port = get_conn_config('neo4j', 'port')
        usr = get_conn_config('neo4j', 'usr')
        pwd = get_conn_config('neo4j', 'pwd')

        driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=(usr, pwd))

        with driver.session() as session:
            res = session.run(f"MATCH (n:{label}) RETURN n ORDER BY n.id DESC " "LIMIT 1")

            data = res.data()

            if data:
                return data[0]['n']['id']
            else:
                return -1

    @staticmethod
    def get_sql_data(table_name, last_id):
        host = get_conn_config('postgres', 'host')
        db = get_conn_config('postgres', 'db')
        usr = get_conn_config('postgres', 'usr')
        pwd = get_conn_config('postgres', 'pwd')

        alchemy_engine = create_engine(f'postgresql+psycopg2://{usr}:{pwd}@{host}/{db}')
        conn = alchemy_engine.connect()

        df = pd.read_sql(f"select * from {table_name} where id > {last_id}", conn)
        df = df.drop(["create_at", "update_at"], axis=1)
        return df

    @staticmethod
    def merge_node(df, label_node, properties):
        def f(row, properties):
            att = dict()

            for prop in properties:
                if row[prop] and (str(row[prop]).lower() not in ['nan', 'none', 'nat']):
                    if isinstance(row[prop], str):
                        valor = row[prop].replace("'", '?')
                        valor = f'"{valor}"'
                    else:
                        valor = row[prop]
                    att[prop] = valor

            att = str(att).replace("'", '').replace("?", "'")
            return att

        df['cypher_merge'] = df.apply(lambda row: f(row, properties), axis=1)
        df['cypher_merge'] = "MERGE (n:" + label_node + " " + df['cypher_merge'] + ")"

        df['cypher_merge'] = (
            df['cypher_merge']
            + "\nON CREATE \n SET n.create_at = timestamp() \nON MATCH \n SET n.update_at = timestamp()"
        )

        return df['cypher_merge'].to_list()

    @staticmethod
    def merge_relationship(df, label_dict, where_items, relationship):
        labels = relationship['labels']
        keys_labels = list(relationship['labels'].keys())

        cypher = "MATCH " + ', '.join([f"({keys}:{labels[keys]})" for keys in labels])
        for idx, item in enumerate(where_items):
            if idx == 0:
                where_c = f'WHERE \n  {where_items[item]} = {item}'
            else:
                where_c += f'\n AND {where_items[item]} = {item}'

        where_c += f"\n AND {list(label_dict.keys())[0]}.id IN " + str(df['id'].to_list())

        merge = f"MERGE ({keys_labels[0]})-[r:{relationship['r_name']} {relationship['r_att']}]->({keys_labels[-1]})"

        return f"{cypher}\n{where_c}\n{merge}"

    @staticmethod
    def send_logger(msg):
        logging.process_info(f"  run cypher query {msg}")
