import sys
from unittest.mock import patch

import pandas as pd

sys.path.append('./app')

from etl import ParseMsg


def base_test(sparklog, df_dataset_res, df_run_res, df_lineage_res):
    df_datasets, df_lineage, df_runs = ParseMsg(sparklog).run()

    if isinstance(df_datasets, pd.core.frame.DataFrame):
        df_dataset_res = df_dataset_res[sorted(list(df_dataset_res.columns))]
        df_datasets = df_datasets[sorted(list(df_datasets.columns))]

        pd.testing.assert_frame_equal(df_dataset_res, df_datasets)

    if isinstance(df_lineage, pd.core.frame.DataFrame):
        df_lineage_res = df_lineage_res[sorted(list(df_lineage_res.columns))]
        df_lineage = df_lineage[sorted(list(df_lineage.columns))]

        pd.testing.assert_frame_equal(df_lineage_res, df_lineage)

    if isinstance(df_runs, pd.core.frame.DataFrame):
        df_run_res = df_run_res[sorted(list(df_run_res.columns))]
        df_runs = df_runs[sorted(list(df_runs.columns))]

        df_runs = df_runs.fillna('0')
        df_run_res = df_run_res.fillna('0')
        pd.testing.assert_frame_equal(df_run_res, df_runs)


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
def test_spark_log_only_input(
    logging,
    add_logging_level,
    spark_log_string_only_input,
    df_datasets_only_input,
    df_lineage_only_input,
    df_runs_only_input,
):
    base_test(
        sparklog=spark_log_string_only_input,
        df_dataset_res=df_datasets_only_input,
        df_run_res=df_runs_only_input,
        df_lineage_res=df_lineage_only_input,
    )


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
def test_spark_log_input_output(
    logging,
    add_logging_level,
    spark_log_string_input_output,
    df_datasets_input_output,
    df_lineage_input_output,
    df_runs_input_output,
):
    base_test(
        sparklog=spark_log_string_input_output,
        df_dataset_res=df_datasets_input_output,
        df_run_res=df_runs_input_output,
        df_lineage_res=df_lineage_input_output,
    )


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
def test_spark_log_only_output(
    logging,
    add_logging_level,
    spark_log_string_only_output,
    df_datasets_only_output,
    df_lineage_only_output,
    df_runs_only_output,
):
    base_test(
        sparklog=spark_log_string_only_output,
        df_dataset_res=df_datasets_only_output,
        df_run_res=df_runs_only_output,
        df_lineage_res=df_lineage_only_output,
    )


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
def test_spark_log_no_input_no_output(
    logging, add_logging_level, spark_log_string_no_input_no_output
):
    base_test(
        sparklog=spark_log_string_no_input_no_output,
        df_dataset_res=None,
        df_run_res=None,
        df_lineage_res=None,
    )


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
def test_spark_log_string_no_complete(logging, add_logging_level, spark_log_string_no_complete):
    base_test(
        sparklog=spark_log_string_no_complete,
        df_dataset_res=None,
        df_run_res=None,
        df_lineage_res=None,
    )


@patch("config.custom_logging.add_logging_level")
@patch("etl.logging")
def test_spark_log_string_delta_operation(
    logging, add_logging_level, spark_log_string_delta_operation
):
    base_test(
        sparklog=spark_log_string_delta_operation,
        df_dataset_res=None,
        df_run_res=None,
        df_lineage_res=None,
    )
