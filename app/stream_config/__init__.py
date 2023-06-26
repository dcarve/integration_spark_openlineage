import logging

from config.env_var import get_conn_config
from kafka import KafkaConsumer

logging.basicConfig(level=70)
logger = logging.getLogger('tasks.read_stream_kafka.py')


class KafkaStream:
    def __init__(self):
        self.KAFKA_TOPIC = get_conn_config('kafka', 'topic')
        self.KAFKA_HOST = get_conn_config('kafka', 'host')
        self.KAFKA_PORT = get_conn_config('kafka', 'port')

        logging.process_info(
            f"  Kafka host: {self.KAFKA_HOST}:{self.KAFKA_PORT}"
            f", Kafka topic: {self.KAFKA_TOPIC}"
        )

    def open_topic(self):
        return KafkaConsumer(
            self.KAFKA_TOPIC,
            bootstrap_servers=f"{self.KAFKA_HOST}:{self.KAFKA_PORT}",
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='openlineage',
            auto_commit_interval_ms=100,
            consumer_timeout_ms=10000,
        )
