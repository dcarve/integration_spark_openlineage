import logging

from config.env_var import get_conn_config
from io_dbms.dags import Dags
from io_dbms.datasets import Datasets
from io_dbms.fields import Fields
from io_dbms.lineage_fields import LineageFields
from io_dbms.runs import Runs
from io_dbms.schemas import Schemas
from io_dbms.tasks import Tasks
from neo4j import GraphDatabase

logging.basicConfig(level=70)
logger = logging.getLogger('io_dbms.query_generator')


class DbmsConfigs:
    def __init__(self):
        self.host = get_conn_config('neo4j', 'host')
        self.port = get_conn_config('neo4j', 'port')
        self.user = get_conn_config('neo4j', 'usr')
        self.password = get_conn_config('neo4j', 'pwd')

        logging.process_info(
            f"  neo4j host: {self.host}:{self.port}"
            f", user: {self.user}"
            f", password: {self.password[0]+'*'*len(self.password[1:])}"
        )


def create_cypher_query():
    logging.process_info("  Converting to cypher")

    dags = Dags()
    tasks = Tasks()
    datasets = Datasets()
    schemas = Schemas()
    fields = Fields()
    runs = Runs()
    lineage_fields = LineageFields()

    queries = [
        dags.convert_to_cypher,
        tasks.convert_to_cypher,
        tasks.convert_to_cypher_relations,
        datasets.convert_to_cypher,
        schemas.convert_to_cypher,
        schemas.convert_to_cypher_relations,
        fields.convert_to_cypher,
        fields.convert_to_cypher_relations,
        runs.convert_to_cypher,
        runs.convert_to_cypher_relations_task,
        runs.convert_to_cypher_relations_dataset_input,
        runs.convert_to_cypher_relations_dataset_output,
        runs.convert_to_cypher_relations_schema_input,
        runs.convert_to_cypher_relations_schema_output,
        lineage_fields.convert_to_cypher,
        lineage_fields.convert_to_cypher_relations_run,
        lineage_fields.convert_to_cypher_relations_field_input,
        lineage_fields.convert_to_cypher_relations_field_output,
    ]

    return queries


def send_data_neo4j(dbms_conn_config, cypher_queries):
    logging.process_info("  Open driver Neo4j")
    driver = GraphDatabase.driver(
        f"bolt://{dbms_conn_config.host}:{dbms_conn_config.port}",
        auth=(dbms_conn_config.user, dbms_conn_config.password),
    )

    session = driver.session()

    for idx, query in enumerate(cypher_queries):
        query_msg = query()
        if query_msg:
            session.run(query_msg)

    logging.process_info("  Close driver Neo4j")
    driver.close()
