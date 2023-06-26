import json
import logging
import os
import re
from re import match

import pandas as pd
from config.env_var import SAVE_LOGS_FOR_DEGUB

logging.basicConfig(level=70)
logger = logging.getLogger('etl')


class PandasApplyLambdaFunctions:
    @staticmethod
    def regex_is_delta_folder(row):
        search_object = re.search(
            r'\/\_delta_log$|\/\_delta_log\/\d{20}.json', row['dataset_name'], flags=re.IGNORECASE
        )

        if search_object:
            return True
        else:
            return False

    @staticmethod
    def field_output_name_adjustment(row):
        field = row["field_name_output"]

        if field.startswith("facets.columnLineage.fields."):
            field = field.replace("facets.columnLineage.fields.", "")
        if field.endswith(".inputFields"):
            field = field.replace(".inputFields", "")

        return field


class ParseMsg:
    def __init__(self, msg):
        self.spark_log = json.loads(msg.value.decode())
        self.timestamp = str(msg.timestamp)

    def run(self):
        job_name = self.spark_log['job']['name'].split('.')[0]
        logging.process_info(f"  Parsing Openlinage Log for job: {job_name}")

        if SAVE_LOGS_FOR_DEGUB:
            if not os.path.isdir("debug"):
                os.makedirs("debug")

            with open(f'debug/{job_name}.{self.timestamp}.json', 'w+') as file:
                file.write(json.dumps(self.spark_log, indent=4))

        df = self.initial_parse(self.spark_log)

        if isinstance(df, pd.core.frame.DataFrame):
            df_inputs = self.parse_input_dataset(df)
            df_outputs = self.parse_output_dataset(df)
            df_lineage = self.parse_lineage_dataset(df)
            df_datasets = self.parse_datasets(df_inputs, df_outputs)

            if isinstance(df_datasets, pd.core.frame.DataFrame):
                df_runs = self.parse_runs(df_datasets)
                return df_datasets, df_lineage, df_runs
            else:
                return None, None, None
        else:
            return None, None, None

    def initial_parse(self, spark_log):
        if (spark_log["eventType"] == "COMPLETE") and (
            spark_log["inputs"] or spark_log["outputs"]
        ):
            df = pd.DataFrame([spark_log])
            df = pd.concat(
                [df, pd.json_normalize(df["run"]), pd.json_normalize(df["job"])], axis=1
            )

            df = df.rename(
                columns={
                    "runId": "run_uuid",
                    "facets.spark.logicalPlan.plan": "spark_logical_plan",
                    "eventTime": "event_time",
                }
            )

            df[["job_name", "action_type"]] = df["name"].str.split(".", n=1, expand=True)
            df[["airflow_env", "dag", "task"]] = df["job_name"].str.split("_", expand=True)

            df["spark_logical_plan"] = df["spark_logical_plan"].astype(str)

            df["spark_logical_plan"] = df["spark_logical_plan"].str.replace(
                pat=r"""\'product-class\'\: \'org.apache.spark.sql.catalyst.expressions.ExprId\', """
                r"""\'id\'\: \d+, \'jvmId\'\: \'[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}\'""",
                repl="",
                regex=True,
            )

            df = df.drop(
                [
                    "facets.spark.logicalPlan._schemaURL",
                    "facets.spark_version._producer",
                    "facets.spark_version.spark-version",
                    "facets.spark_version._schemaURL",
                    "facets.spark_version.openlineage-spark-version",
                    "facets.spark.logicalPlan._producer",
                    "eventType",
                    "producer",
                    "schemaURL",
                    "run",
                    "job",
                    "namespace",
                ],
                axis=1,
            )

            return df

    def parse_input_dataset(self, df):
        df_inputs = (
            df[
                [
                    "event_time",
                    "job_name",
                    "airflow_env",
                    "dag",
                    "task",
                    "action_type",
                    "run_uuid",
                    "spark_logical_plan",
                    "inputs",
                ]
            ]
            .explode("inputs")
            .reset_index(drop=True)
        )

        if df_inputs[df_inputs['inputs'].notnull()].shape[0] > 0:
            logging.process_info("  has input")

            df_inputs = pd.concat([df_inputs, pd.json_normalize(df_inputs["inputs"])], axis=1)

            df_inputs = df_inputs.rename(
                columns={
                    "facets.schema.fields": "dataset_schema",
                    "namespace": "dataset_namespace",
                    "name": "dataset_name",
                    "facets.dataSource.uri": "dataset_uri",
                    "facets.dataSource.name": "dataset_location",
                }
            ).drop(
                [
                    "facets.dataSource._producer",
                    "facets.dataSource._schemaURL",
                    "facets.schema._schemaURL",
                    "facets.schema._producer",
                    "inputs",
                ],
                axis=1,
            )

            df_inputs["input_output"] = "input"
            df_inputs["dataset_schema_json"] = df_inputs["dataset_schema"].astype(str)

            df_inputs['delete_row'] = df_inputs.apply(
                lambda row: PandasApplyLambdaFunctions.regex_is_delta_folder(row), axis=1
            )

            df_inputs = df_inputs[df_inputs.delete_row == False]
            df_inputs = df_inputs.drop(['delete_row'], axis=1)

            if df_inputs.shape[0] > 0:
                return df_inputs
            else:
                return None

        else:
            return None

    def parse_output_dataset(self, df):
        df_outputs = (
            df[
                [
                    "event_time",
                    "job_name",
                    "airflow_env",
                    "dag",
                    "task",
                    "action_type",
                    "run_uuid",
                    "spark_logical_plan",
                    "outputs",
                ]
            ]
            .explode("outputs")
            .reset_index(drop=True)
        )

        if df_outputs[df_outputs['outputs'].notnull()].shape[0] > 0:
            logging.process_info("  has output")

            df_outputs = pd.concat([df_outputs, pd.json_normalize(df_outputs["outputs"])], axis=1)

            df_outputs = df_outputs.rename(
                columns={
                    "facets.schema.fields": "dataset_schema",
                    "namespace": "dataset_namespace",
                    "name": "dataset_name",
                    "facets.dataSource.uri": "dataset_uri",
                    "facets.dataSource.name": "dataset_location",
                }
            ).drop(
                [
                    "facets.dataSource._producer",
                    "facets.dataSource._schemaURL",
                    "facets.schema._schemaURL",
                    "facets.schema._producer",
                    "outputs",
                ],
                axis=1,
            )

            if "facets.storage.storageLayer" in list(df_outputs.columns):
                df_outputs = df_outputs[df_outputs["facets.storage.storageLayer"] != "delta"]

            if df_outputs.shape[0] > 0:
                df_outputs['delete_row'] = df_outputs.apply(
                    lambda row: PandasApplyLambdaFunctions.regex_is_delta_folder(row), axis=1
                )

                df_outputs = df_outputs[df_outputs.delete_row == False]
                df_outputs = df_outputs.drop(['delete_row'], axis=1)

            if df_outputs.shape[0] > 0:
                df_outputs = df_outputs[
                    [
                        "event_time",
                        "job_name",
                        "airflow_env",
                        "dag",
                        "task",
                        "action_type",
                        "run_uuid",
                        "spark_logical_plan",
                        "dataset_namespace",
                        "dataset_name",
                        "dataset_location",
                        "dataset_uri",
                        "dataset_schema",
                    ]
                ]

                df_outputs["input_output"] = "output"
                df_outputs["dataset_schema_json"] = df_outputs["dataset_schema"].astype(str)

                return df_outputs

        else:
            return None

    def parse_lineage_dataset(self, df):
        df_lineage = (
            df[
                [
                    "event_time",
                    "job_name",
                    "airflow_env",
                    "dag",
                    "task",
                    "action_type",
                    "run_uuid",
                    "spark_logical_plan",
                    "outputs",
                ]
            ]
            .explode("outputs")
            .reset_index(drop=True)
        )

        if df_lineage[df_lineage['outputs'].notnull()].shape[0] > 0:
            df_lineage = pd.concat([df_lineage, pd.json_normalize(df_lineage["outputs"])], axis=1)

            if list(filter(lambda v: match('facets.columnLineage.fields', v), df_lineage.columns)):
                df_lineage = df_lineage.rename(
                    columns={
                        "facets.schema.fields": "dataset_schema",
                        "namespace": "dataset_namespace",
                        "name": "dataset_name",
                        "facets.dataSource.uri": "dataset_uri",
                        "facets.dataSource.name": "dataset_location",
                    }
                ).drop(
                    [
                        "facets.dataSource._producer",
                        "facets.dataSource._schemaURL",
                        "facets.schema._schemaURL",
                        "facets.schema._producer",
                        "outputs",
                    ],
                    axis=1,
                )

                df_lineage["dataset_schema_json"] = df_lineage["dataset_schema"].astype(str)

                if "facets.storage.storageLayer" in list(df_lineage.columns):
                    df_lineage = df_lineage[df_lineage["facets.storage.storageLayer"] != "delta"]

                if df_lineage.shape[0] > 0:
                    df_lineage['delete_row'] = df_lineage.apply(
                        lambda row: PandasApplyLambdaFunctions.regex_is_delta_folder(row), axis=1
                    )

                    df_lineage = df_lineage[df_lineage.delete_row == False]
                    df_lineage = df_lineage.drop(['delete_row'], axis=1)

                if df_lineage.shape[0] > 0:
                    columns = list(df_lineage.columns)

                    fix_columns = [
                        "event_time",
                        "job_name",
                        "airflow_env",
                        "dag",
                        "task",
                        "action_type",
                        "run_uuid",
                        "spark_logical_plan",
                        "dataset_namespace",
                        "dataset_name",
                        "dataset_uri",
                        "dataset_location",
                        "dataset_schema_json",
                    ]

                    columns_lineage = [
                        cols for cols in columns if "facets.columnLineage.fields" in cols
                    ]

                    df_lineage = df_lineage.melt(
                        id_vars=fix_columns,
                        value_vars=columns_lineage,
                        var_name="field_name_output",
                        value_name="column_lineage",
                    )

                    df_lineage["field_name_output"] = df_lineage.apply(
                        lambda row: PandasApplyLambdaFunctions.field_output_name_adjustment(row),
                        axis=1,
                    )

                    df_lineage = df_lineage.explode("column_lineage").reset_index(drop=True)

                    df_lineage = (
                        pd.concat(
                            [df_lineage, pd.json_normalize(df_lineage["column_lineage"])],
                            axis=1,
                        )
                        .rename(
                            columns={
                                "namespace": "namespace_origin",
                                "name": "dataset_name_origin",
                                "field": "field_name_origin",
                            }
                        )
                        .drop("column_lineage", axis=1)
                    )

                    df_lineage["dataset_name"] = df_lineage["dataset_name"].str.strip("/")
                    df_lineage["dataset_name_origin"] = df_lineage[
                        "dataset_name_origin"
                    ].str.strip("/")

                    return df_lineage
                else:
                    return None
            else:
                return None

    def parse_datasets(self, df_inputs, df_outputs):
        if isinstance(df_inputs, pd.core.frame.DataFrame) and isinstance(
            df_outputs, pd.core.frame.DataFrame
        ):
            df = pd.concat([df_inputs, df_outputs])
        elif isinstance(df_inputs, pd.core.frame.DataFrame) and not isinstance(
            df_outputs, pd.core.frame.DataFrame
        ):
            df = df_inputs
        elif not isinstance(df_inputs, pd.core.frame.DataFrame) and isinstance(
            df_outputs, pd.core.frame.DataFrame
        ):
            df = df_outputs

        else:
            df = None

        if isinstance(df, pd.core.frame.DataFrame):
            df = df[
                df[["dataset_uri"]].notnull().all(1) & df[["dataset_location"]].notnull().all(1)
            ]

            df = df.explode("dataset_schema").reset_index(drop=True)

            df = (
                pd.concat([df, pd.json_normalize(df["dataset_schema"])], axis=1)
                .rename(
                    columns={
                        "name": "dataset_schema_field_name",
                        "type": "dataset_schema_field_type",
                    }
                )
                .drop("dataset_schema", axis=1)
            )

            df["dataset_name"] = df["dataset_name"].str.strip("/")

            return df

        else:
            return None

    def parse_runs(self, df):
        df_runs = (
            df[
                [
                    "event_time",
                    "job_name",
                    "airflow_env",
                    "dag",
                    "task",
                    "action_type",
                    "run_uuid",
                    "spark_logical_plan",
                    "input_output",
                    "dataset_namespace",
                    "dataset_name",
                    "dataset_uri",
                    "dataset_location",
                    "dataset_schema_json",
                ]
            ]
            .rename(
                columns={
                    "dataset_namespace": "dataset_namespace_input",
                    "dataset_name": "dataset_name_input",
                    "dataset_uri": "dataset_uri_input",
                    "dataset_location": "dataset_location_input",
                    "dataset_schema_json": "dataset_schema_json_input",
                }
            )
            .drop_duplicates()[df.input_output == "input"]
            .merge(
                df[
                    [
                        "event_time",
                        "job_name",
                        "airflow_env",
                        "dag",
                        "task",
                        "action_type",
                        "run_uuid",
                        "spark_logical_plan",
                        "input_output",
                        "dataset_namespace",
                        "dataset_name",
                        "dataset_uri",
                        "dataset_location",
                        "dataset_schema_json",
                    ]
                ]
                .rename(
                    columns={
                        "dataset_namespace": "dataset_namespace_output",
                        "dataset_name": "dataset_name_output",
                        "dataset_uri": "dataset_uri_output",
                        "dataset_location": "dataset_location_output",
                        "dataset_schema_json": "dataset_schema_json_output",
                    }
                )
                .drop_duplicates()[df.input_output == "output"],
                on=[
                    "event_time",
                    "job_name",
                    "airflow_env",
                    "dag",
                    "task",
                    "action_type",
                    "run_uuid",
                    "spark_logical_plan",
                ],
                how="outer",
            )
        )

        return df_runs
