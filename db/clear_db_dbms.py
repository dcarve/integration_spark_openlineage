import sys
import logging
sys.path.append('./app')

from config.custom_logging import add_logging_level

add_logging_level('PROCESS_INFO', 70)
logging.basicConfig(level=70)
logger = logging.getLogger('main')

from io_db.query_generator import DbConfigs, send_data_db
from io_dbms.query_generator import DbmsConfigs, send_data_neo4j


clear_db = open('db/clear.sql').read()
setup_db = open('db/setup.sql').read()

def query_neo4j():
    return "match (n) detach delete n"

send_data_db(DbConfigs(), [clear_db,setup_db])
logging.process_info("  all data has been deleted from db")

send_data_neo4j(DbmsConfigs(), [query_neo4j])
logging.process_info("  all data has been deleted from dbms")