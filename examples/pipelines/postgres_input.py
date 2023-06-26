import argparse
import re

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

arguments = argparse.ArgumentParser()
arguments.add_argument("--openlineagejar", type=str, required=True)
args = arguments.parse_args()

openlineagejar = args.openlineagejar

server = 'http:/localhost:9092'

taskname = re.sub(r'[^a-zA-Z0-9]', '', openlineagejar)

builder = (
    SparkSession.builder.appName(f's-postgresinput-{taskname}')
    .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
    .config("spark.openlineage.transport.type", "kafka")
    .config("spark.openlineage.transport.topicName", "openlineage")
    .config("spark.openlineage.transport.localServerId", server)
    .config("spark.openlineage.transport.properties.bootstrap.servers", server)
    .config('spark.openlineage.namespace', 'spark_teste')
    .config(
        "spark.openlineage.transport.properties.key.serializer",
        'org.apache.kafka.common.serialization.StringSerializer',
    )
    .config(
        "spark.openlineage.transport.properties.value.serializer",
        'org.apache.kafka.common.serialization.StringSerializer',
    )
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "False")
    .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "True")
)


spark = configure_spark_with_delta_pip(
    builder, extra_packages=[openlineagejar, 'org.postgresql:postgresql:42.2.5']
).getOrCreate()

POSTGRES_DATABASE = "openlineage"
POSTGRES_PASSWORD = "openlineage"
POSTGRES_USER = "openlineage"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "staging.example")
    .option("user", f"{POSTGRES_USER}")
    .option("password", f"{POSTGRES_PASSWORD}")
    .load()
)

df_p = df.toPandas()

spark.stop()
