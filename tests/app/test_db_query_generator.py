import sys
from unittest.mock import patch

sys.path.append('./app')

from etl import ParseMsg
from io_db.query_generator import create_sql_query


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
@patch("io_db.query_generator.logging")
def test_sql(
    logging_1,
    logging_2,
    add_logging_level,
    spark_log_string_input_output,
    dags_query_input_output,
    task_query_input_output,
    datasets_query_input_output,
    schemas_query_input_output,
    fields_query_input_output,
    runs_query_input_output,
    lineage_fields_query_input_output,
):
    df_datasets, df_lineage, df_runs = ParseMsg(spark_log_string_input_output).run()
    queries_sql = create_sql_query(df_datasets, df_lineage, df_runs)

    for querie in queries_sql:
        if 'INSERT INTO openlineage.dags' in querie:
            assert " ".join(querie.split()) == " ".join(dags_query_input_output.split())
        elif 'INSERT INTO openlineage.tasks' in querie:
            assert " ".join(querie.split()) == " ".join(task_query_input_output.split())
        elif 'INSERT INTO openlineage.datasets' in querie:
            assert " ".join(querie.split()) == " ".join(datasets_query_input_output.split())
        elif 'INSERT INTO openlineage.schemas' in querie:
            assert " ".join(querie.split()) == " ".join(schemas_query_input_output.split())
        elif 'INSERT INTO openlineage.fields' in querie:
            assert " ".join(querie.split()) == " ".join(fields_query_input_output.split())
        elif 'INSERT INTO openlineage.runs' in querie:
            assert " ".join(querie.split()) == " ".join(runs_query_input_output.split())
        elif 'INSERT INTO openlineage.lineage_fields' in querie:
            assert " ".join(querie.split()) == " ".join(lineage_fields_query_input_output.split())
