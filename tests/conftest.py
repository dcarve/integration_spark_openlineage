import json

import numpy as np
import pandas as pd
import pytest

#############################  SPARK LOGS STRING  #######################################


@pytest.fixture()
def spark_log_string_no_complete():
    log = {
        "eventType": "START",
        "eventTime": "2023-05-06T00:11:19.742Z",
        "run": {},
        "job": {
            "namespace": "default",
            "name": "s_dagname_taskname.columnar_to_row",
            "facets": {},
        },
        "inputs": [],
        "outputs": [],
    }

    class msg:
        value = json.dumps(log).encode()
        timestamp = 12

    return msg


@pytest.fixture()
def spark_log_string_no_input_no_output():
    log = {
        "eventType": "COMPLETE",
        "eventTime": "2023-05-06T00:11:19.742Z",
        "run": {},
        "job": {
            "namespace": "default",
            "name": "s_dagname_taskname.columnar_to_row",
            "facets": {},
        },
        "inputs": [],
        "outputs": [],
    }

    class msg:
        value = json.dumps(log).encode()
        timestamp = 12

    return msg


@pytest.fixture()
def spark_log_string_only_input():
    log = {
        "eventType": "COMPLETE",
        "eventTime": "2023-05-06T00:11:19.742Z",
        "run": {
            "runId": "c89daa07-3ef6-4030-908e-95c12fa4791a",
            "facets": {
                "spark.logicalPlan": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "plan": [{}],
                },
                "spark_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "spark-version": "3.2.4",
                    "openlineage-spark-version": "0.21.1",
                },
            },
        },
        "job": {
            "namespace": "default",
            "name": "s_dagname_taskname.columnar_to_row",
            "facets": {},
        },
        "inputs": [
            {
                "namespace": "gs://stg-lake-raw-ephemeral",
                "name": "/open_lineage_delta_input",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "gs://stg-lake-raw-ephemeral",
                        "uri": "gs://stg-lake-raw-ephemeral",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {"name": "ano", "type": "long"},
                            {"name": "inflacao", "type": "double"},
                        ],
                    },
                },
                "inputFacets": {},
            }
        ],
        "outputs": [],
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }

    class msg:
        value = json.dumps(log).encode()
        timestamp = 12

    return msg


@pytest.fixture()
def spark_log_string_input_output():
    log = {
        "eventType": "COMPLETE",
        "eventTime": "2023-05-06T00:11:26.569Z",
        "run": {
            "runId": "de3ee8b8-c16a-430d-97fa-6ee57f7dabde",
            "facets": {
                "spark.logicalPlan": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "plan": [{}],
                },
                "spark_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "spark-version": "3.2.4",
                    "openlineage-spark-version": "0.21.1",
                },
            },
        },
        "job": {
            "namespace": "default",
            "name": "s_dagname_taskname.execute_save_into_data_source_command",
            "facets": {},
        },
        "inputs": [
            {
                "namespace": "gs://stg-lake-raw-ephemeral",
                "name": "/open_lineage_delta_input",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "gs://stg-lake-raw-ephemeral",
                        "uri": "gs://stg-lake-raw-ephemeral",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {"name": "ano", "type": "long"},
                            {"name": "inflacao", "type": "double"},
                        ],
                    },
                },
                "inputFacets": {},
            }
        ],
        "outputs": [
            {
                "namespace": "gs://stg-lake-refined-ephemeral",
                "name": "/open_lineage_versions/",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "gs://stg-lake-refined-ephemeral",
                        "uri": "gs://stg-lake-refined-ephemeral",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {"name": "ano", "type": "long"},
                            {"name": "inflacao", "type": "double"},
                        ],
                    },
                    "columnLineage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
                        "fields": {
                            "ano": {
                                "inputFields": [
                                    {
                                        "namespace": "gs://stg-lake-raw-ephemeral",
                                        "name": "/open_lineage_delta_input",
                                        "field": "ano",
                                    }
                                ]
                            },
                            "inflacao": {
                                "inputFields": [
                                    {
                                        "namespace": "gs://stg-lake-raw-ephemeral",
                                        "name": "/open_lineage_delta_input",
                                        "field": "inflacao",
                                    }
                                ]
                            },
                        },
                    },
                    "lifecycleStateChange": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
                        "lifecycleStateChange": "OVERWRITE",
                    },
                },
                "outputFacets": {},
            }
        ],
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }

    class msg:
        value = json.dumps(log).encode()
        timestamp = 12

    return msg


@pytest.fixture()
def spark_log_string_delta_operation():
    log = {
        "eventType": "COMPLETE",
        "eventTime": "2023-05-06T00:11:27.163Z",
        "run": {
            "runId": "8d09f948-df89-4e72-a30e-e0fe193f34dd",
            "facets": {
                "spark.logicalPlan": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "plan": [{}],
                },
                "spark_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "spark-version": "3.2.4",
                    "openlineage-spark-version": "0.21.1",
                },
            },
        },
        "job": {"namespace": "default", "name": "s_dagname_taskname.create_table", "facets": {}},
        "inputs": [],
        "outputs": [
            {
                "namespace": "gs://stg-lake-refined-ephemeral",
                "name": "open_lineage_versions",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "gs://stg-lake-refined-ephemeral",
                        "uri": "gs://stg-lake-refined-ephemeral",
                    },
                    "version": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet",
                        "datasetVersion": "0",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {"name": "ano", "type": "long"},
                            {"name": "inflacao", "type": "double"},
                        ],
                    },
                    "storage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json#/$defs/StorageDatasetFacet",
                        "storageLayer": "delta",
                        "fileFormat": "parquet",
                    },
                    "symlinks": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                        "identifiers": [
                            {
                                "namespace": "open_lineage_versions",
                                "name": "ephemeral_stg_refined.open_lineage_versions",
                                "type": "TABLE",
                            }
                        ],
                    },
                    "lifecycleStateChange": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
                        "lifecycleStateChange": "CREATE",
                    },
                },
                "outputFacets": {},
            }
        ],
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }

    class msg:
        value = json.dumps(log).encode()
        timestamp = 12

    return msg


@pytest.fixture()
def spark_log_string_only_output():
    log = {
        "eventType": "COMPLETE",
        "eventTime": "2023-05-02T19:09:17.374Z",
        "run": {
            "runId": "272a9202-728c-4915-9721-55acf147b154",
            "facets": {
                "spark.logicalPlan": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "plan": [{}],
                },
                "spark_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet",
                    "spark-version": "3.2.4",
                    "openlineage-spark-version": "0.21.1",
                },
            },
        },
        "job": {
            "namespace": "default",
            "name": "s_dagname_taskname.execute_insert_into_hadoop_fs_relation_command",
            "facets": {},
        },
        "inputs": [],
        "outputs": [
            {
                "namespace": "gs://stg-lake-raw-ephemeral",
                "name": "open_lineage_parquet_output_from_pandas",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "gs://stg-lake-raw-ephemeral",
                        "uri": "gs://stg-lake-raw-ephemeral",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {"name": "ano", "type": "long"},
                            {"name": "inflacao", "type": "double"},
                        ],
                    },
                    "lifecycleStateChange": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
                        "lifecycleStateChange": "OVERWRITE",
                    },
                },
                "outputFacets": {},
            }
        ],
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/0.21.1/integration/spark",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }

    class msg:
        value = json.dumps(log).encode()
        timestamp = 12

    return msg


#############################  DF DATASETS  #######################################


@pytest.fixture()
def df_datasets_only_input():
    return pd.DataFrame(
        {
            "event_time": ['2023-05-06T00:11:19.742Z', '2023-05-06T00:11:19.742Z'],
            "job_name": ['s_dagname_taskname', 's_dagname_taskname'],
            "airflow_env": ['s', 's'],
            "dag": ['dagname', 'dagname'],
            "task": ['taskname', 'taskname'],
            "action_type": ['columnar_to_row', 'columnar_to_row'],
            "run_uuid": [
                'c89daa07-3ef6-4030-908e-95c12fa4791a',
                'c89daa07-3ef6-4030-908e-95c12fa4791a',
            ],
            "spark_logical_plan": ["[{}]", "[{}]"],
            "dataset_namespace": ['gs://stg-lake-raw-ephemeral', 'gs://stg-lake-raw-ephemeral'],
            "dataset_name": ['open_lineage_delta_input', 'open_lineage_delta_input'],
            "dataset_location": ['gs://stg-lake-raw-ephemeral', 'gs://stg-lake-raw-ephemeral'],
            "dataset_uri": ['gs://stg-lake-raw-ephemeral', 'gs://stg-lake-raw-ephemeral'],
            "input_output": ['input', 'input'],
            "dataset_schema_json": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
            ],
            "dataset_schema_field_name": ['ano', 'inflacao'],
            "dataset_schema_field_type": ['long', 'double'],
        }
    )


@pytest.fixture()
def df_datasets_input_output():
    return pd.DataFrame(
        {
            "event_time": [
                '2023-05-06T00:11:26.569Z',
                '2023-05-06T00:11:26.569Z',
                '2023-05-06T00:11:26.569Z',
                '2023-05-06T00:11:26.569Z',
            ],
            "job_name": [
                's_dagname_taskname',
                's_dagname_taskname',
                's_dagname_taskname',
                's_dagname_taskname',
            ],
            "airflow_env": ['s', 's', 's', 's'],
            "dag": ['dagname', 'dagname', 'dagname', 'dagname'],
            "task": ['taskname', 'taskname', 'taskname', 'taskname'],
            "action_type": [
                'execute_save_into_data_source_command',
                'execute_save_into_data_source_command',
                'execute_save_into_data_source_command',
                'execute_save_into_data_source_command',
            ],
            "run_uuid": [
                'de3ee8b8-c16a-430d-97fa-6ee57f7dabde',
                'de3ee8b8-c16a-430d-97fa-6ee57f7dabde',
                'de3ee8b8-c16a-430d-97fa-6ee57f7dabde',
                'de3ee8b8-c16a-430d-97fa-6ee57f7dabde',
            ],
            "spark_logical_plan": ["[{}]", "[{}]", "[{}]", "[{}]"],
            "dataset_namespace": [
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-refined-ephemeral',
                'gs://stg-lake-refined-ephemeral',
            ],
            "dataset_name": [
                'open_lineage_delta_input',
                'open_lineage_delta_input',
                'open_lineage_versions',
                'open_lineage_versions',
            ],
            "dataset_location": [
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-refined-ephemeral',
                'gs://stg-lake-refined-ephemeral',
            ],
            "dataset_uri": [
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-refined-ephemeral',
                'gs://stg-lake-refined-ephemeral',
            ],
            "input_output": ['input', 'input', 'output', 'output'],
            "dataset_schema_json": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
            ],
            "dataset_schema_field_name": ['ano', 'inflacao', 'ano', 'inflacao'],
            "dataset_schema_field_type": ['long', 'double', 'long', 'double'],
        }
    )


@pytest.fixture()
def df_datasets_only_output():
    return pd.DataFrame(
        {
            "event_time": [
                '2023-05-02T19:09:17.374Z',
                '2023-05-02T19:09:17.374Z',
            ],
            "job_name": [
                's_dagname_taskname',
                's_dagname_taskname',
            ],
            "airflow_env": ['s', 's'],
            "dag": ['dagname', 'dagname'],
            "task": [
                'taskname',
                'taskname',
            ],
            "action_type": [
                'execute_insert_into_hadoop_fs_relation_command',
                'execute_insert_into_hadoop_fs_relation_command',
            ],
            "run_uuid": [
                '272a9202-728c-4915-9721-55acf147b154',
                '272a9202-728c-4915-9721-55acf147b154',
            ],
            "spark_logical_plan": ["[{}]", "[{}]"],
            "dataset_namespace": [
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-raw-ephemeral',
            ],
            "dataset_name": [
                'open_lineage_parquet_output_from_pandas',
                'open_lineage_parquet_output_from_pandas',
            ],
            "dataset_location": [
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-raw-ephemeral',
            ],
            "dataset_uri": [
                'gs://stg-lake-raw-ephemeral',
                'gs://stg-lake-raw-ephemeral',
            ],
            "input_output": ['output', 'output'],
            "dataset_schema_json": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
            ],
            "dataset_schema_field_name": ['ano', 'inflacao'],
            "dataset_schema_field_type": [
                'long',
                'double',
            ],
        }
    )


#############################  DF RUNS  #######################################


@pytest.fixture()
def df_runs_only_input():
    return pd.DataFrame(
        {
            "event_time": ['2023-05-06T00:11:19.742Z'],
            "job_name": ['s_dagname_taskname'],
            "airflow_env": ['s'],
            "dag": ['dagname'],
            "task": ['taskname'],
            "action_type": ['columnar_to_row'],
            "run_uuid": ['c89daa07-3ef6-4030-908e-95c12fa4791a'],
            "spark_logical_plan": ["[{}]"],
            "input_output_x": ['input'],
            "dataset_namespace_input": ['gs://stg-lake-raw-ephemeral'],
            "dataset_name_input": ['open_lineage_delta_input'],
            "dataset_uri_input": ['gs://stg-lake-raw-ephemeral'],
            "dataset_location_input": ['gs://stg-lake-raw-ephemeral'],
            "dataset_schema_json_input": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
            ],
            "input_output_y": [np.nan],
            "dataset_namespace_output": [np.nan],
            "dataset_name_output": [np.nan],
            "dataset_uri_output": [np.nan],
            "dataset_location_output": [np.nan],
            "dataset_schema_json_output": [np.nan],
        }
    )


@pytest.fixture()
def df_runs_input_output():
    return pd.DataFrame(
        {
            "event_time": ['2023-05-06T00:11:26.569Z'],
            "job_name": ['s_dagname_taskname'],
            "airflow_env": ['s'],
            "dag": ['dagname'],
            "task": ['taskname'],
            "action_type": ['execute_save_into_data_source_command'],
            "run_uuid": ['de3ee8b8-c16a-430d-97fa-6ee57f7dabde'],
            "spark_logical_plan": ["[{}]"],
            "input_output_x": ['input'],
            "dataset_namespace_input": ['gs://stg-lake-raw-ephemeral'],
            "dataset_name_input": ['open_lineage_delta_input'],
            "dataset_uri_input": ['gs://stg-lake-raw-ephemeral'],
            "dataset_location_input": ['gs://stg-lake-raw-ephemeral'],
            "dataset_schema_json_input": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
            ],
            "input_output_y": ['output'],
            "dataset_namespace_output": ['gs://stg-lake-refined-ephemeral'],
            "dataset_name_output": ['open_lineage_versions'],
            "dataset_uri_output": ['gs://stg-lake-refined-ephemeral'],
            "dataset_location_output": ['gs://stg-lake-refined-ephemeral'],
            "dataset_schema_json_output": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]"
            ],
        }
    )


@pytest.fixture()
def df_runs_only_output():
    return pd.DataFrame(
        {
            "event_time": ['2023-05-02T19:09:17.374Z'],
            "job_name": ['s_dagname_taskname'],
            "airflow_env": ['s'],
            "dag": ['dagname'],
            "task": ['taskname'],
            "action_type": ['execute_insert_into_hadoop_fs_relation_command'],
            "run_uuid": ['272a9202-728c-4915-9721-55acf147b154'],
            "spark_logical_plan": ["[{}]"],
            "input_output_x": [np.nan],
            "dataset_namespace_input": [np.nan],
            "dataset_name_input": [np.nan],
            "dataset_uri_input": [np.nan],
            "dataset_location_input": [np.nan],
            "dataset_schema_json_input": [np.nan],
            "input_output_y": ['output'],
            "dataset_namespace_output": ['gs://stg-lake-raw-ephemeral'],
            "dataset_name_output": ['open_lineage_parquet_output_from_pandas'],
            "dataset_uri_output": ['gs://stg-lake-raw-ephemeral'],
            "dataset_location_output": ['gs://stg-lake-raw-ephemeral'],
            "dataset_schema_json_output": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]"
            ],
        }
    )


#############################  DF LINEAGE  #######################################


@pytest.fixture()
def df_lineage_only_input():
    return None


@pytest.fixture()
def df_lineage_input_output():
    return pd.DataFrame(
        {
            "event_time": ['2023-05-06T00:11:26.569Z', '2023-05-06T00:11:26.569Z'],
            "job_name": ['s_dagname_taskname', 's_dagname_taskname'],
            "airflow_env": ['s', 's'],
            "dag": ['dagname', 'dagname'],
            "task": ['taskname', 'taskname'],
            "action_type": [
                'execute_save_into_data_source_command',
                'execute_save_into_data_source_command',
            ],
            "run_uuid": [
                'de3ee8b8-c16a-430d-97fa-6ee57f7dabde',
                'de3ee8b8-c16a-430d-97fa-6ee57f7dabde',
            ],
            "spark_logical_plan": [
                "[{}]",
                "[{}]",
            ],
            "dataset_namespace": [
                'gs://stg-lake-refined-ephemeral',
                'gs://stg-lake-refined-ephemeral',
            ],
            "dataset_name": ['open_lineage_versions', 'open_lineage_versions'],
            "dataset_uri": ['gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral'],
            "dataset_location": [
                'gs://stg-lake-refined-ephemeral',
                'gs://stg-lake-refined-ephemeral',
            ],
            "dataset_schema_json": [
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
                "[{'name': 'ano', 'type': 'long'}, {'name': 'inflacao', 'type': 'double'}]",
            ],
            "field_name_output": ["ano", "inflacao"],
            "namespace_origin": ["gs://stg-lake-raw-ephemeral", "gs://stg-lake-raw-ephemeral"],
            "dataset_name_origin": ["open_lineage_delta_input", "open_lineage_delta_input"],
            "field_name_origin": ["ano", "inflacao"],
        }
    )


@pytest.fixture()
def df_lineage_only_output():
    return None


#############################  DB QUERIES  #######################################


@pytest.fixture()
def dags_query_input_output():
    return "WITH insert_statement AS (SELECT tmp.dag AS name, tmp.airflow_env AS airflow_env, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'dagname' AS dag, 's' AS airflow_env) tmp LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env WHERE (openlineage.dags.name ISNULL AND openlineage.dags.airflow_env ISNULL) ) INSERT INTO openlineage.dags (name, airflow_env, create_at, update_at) SELECT name, airflow_env, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT tmp.dag AS name, tmp.airflow_env AS airflow_env, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'dagname' AS dag, 's' AS airflow_env) tmp LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env WHERE NOT (openlineage.dags.name ISNULL AND openlineage.dags.airflow_env ISNULL) ) UPDATE openlineage.dags SET update_at=CURRENT_TIMESTAMP FROM (SELECT name, airflow_env FROM update_statement) AS subquery WHERE openlineage.dags.name=subquery.name AND openlineage.dags.airflow_env=subquery.airflow_env ;"


@pytest.fixture()
def task_query_input_output():
    return "WITH insert_statement AS (SELECT tmp.task AS name, openlineage.dags.id AS dag_id, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'dagname' AS dag, 's' AS airflow_env, 'taskname' AS task) tmp LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env LEFT JOIN openlineage.tasks ON openlineage.tasks.dag_id = openlineage.dags.id AND openlineage.tasks.name = tmp.task WHERE (openlineage.tasks.dag_id ISNULL AND openlineage.tasks.name ISNULL) ) INSERT INTO openlineage.tasks (name, dag_id, create_at, update_at) SELECT name, dag_id, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT tmp.task AS name, openlineage.dags.id AS dag_id, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'dagname' AS dag, 's' AS airflow_env, 'taskname' AS task) tmp LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env LEFT JOIN openlineage.tasks ON openlineage.tasks.dag_id = openlineage.dags.id AND openlineage.tasks.name = tmp.task WHERE NOT (openlineage.tasks.dag_id ISNULL AND openlineage.tasks.name ISNULL) ) UPDATE openlineage.tasks SET update_at=CURRENT_TIMESTAMP FROM (SELECT name, dag_id FROM update_statement) AS subquery WHERE openlineage.tasks.name=subquery.name AND openlineage.tasks.dag_id=subquery.dag_id ;"


@pytest.fixture()
def datasets_query_input_output():
    return "WITH insert_statement AS (SELECT tmp.dataset_location AS LOCATION, tmp.dataset_uri AS uri, tmp.dataset_name AS name, tmp.dataset_namespace AS namespace, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'gs://stg-lake-raw-ephemeral' AS dataset_location, 'gs://stg-lake-raw-ephemeral' AS dataset_uri, 'open_lineage_delta_input' AS dataset_name, 'gs://stg-lake-raw-ephemeral' AS dataset_namespace UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral')) tmp LEFT JOIN openlineage.datasets ON openlineage.datasets.name = tmp.dataset_name AND openlineage.datasets.uri = tmp.dataset_uri AND openlineage.datasets.location = tmp.dataset_location AND openlineage.datasets.namespace = tmp.dataset_namespace WHERE (openlineage.datasets.name ISNULL AND openlineage.datasets.uri ISNULL AND openlineage.datasets.location ISNULL AND openlineage.datasets.namespace ISNULL) ) INSERT INTO openlineage.datasets (LOCATION, uri, name, namespace, create_at, update_at) SELECT LOCATION, uri, name, namespace, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT tmp.dataset_location AS LOCATION, tmp.dataset_uri AS uri, tmp.dataset_name AS name, tmp.dataset_namespace AS namespace, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'gs://stg-lake-raw-ephemeral' AS dataset_location, 'gs://stg-lake-raw-ephemeral' AS dataset_uri, 'open_lineage_delta_input' AS dataset_name, 'gs://stg-lake-raw-ephemeral' AS dataset_namespace UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral')) tmp LEFT JOIN openlineage.datasets ON openlineage.datasets.name = tmp.dataset_name AND openlineage.datasets.uri = tmp.dataset_uri AND openlineage.datasets.location = tmp.dataset_location AND openlineage.datasets.namespace = tmp.dataset_namespace WHERE NOT (openlineage.datasets.name ISNULL AND openlineage.datasets.uri ISNULL AND openlineage.datasets.location ISNULL AND openlineage.datasets.namespace ISNULL) ) UPDATE openlineage.datasets SET update_at=CURRENT_TIMESTAMP FROM (SELECT LOCATION, uri, name, namespace FROM update_statement) AS subquery WHERE openlineage.datasets.location=subquery.location AND openlineage.datasets.uri=subquery.uri AND openlineage.datasets.name=subquery.name AND openlineage.datasets.namespace=subquery.namespace ;"


@pytest.fixture()
def schemas_query_input_output():
    return "WITH insert_statement AS (SELECT tmp.dataset_schema_json AS schema_json, openlineage.datasets.id AS dataset_id, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'gs://stg-lake-raw-ephemeral' AS dataset_location, 'gs://stg-lake-raw-ephemeral' AS dataset_uri, 'open_lineage_delta_input' AS dataset_name, 'gs://stg-lake-raw-ephemeral' AS dataset_namespace, '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]' AS dataset_schema_json UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]')) tmp LEFT JOIN openlineage.datasets ON openlineage.datasets.name = tmp.dataset_name AND openlineage.datasets.uri = tmp.dataset_uri AND openlineage.datasets.location = tmp.dataset_location AND openlineage.datasets.namespace = tmp.dataset_namespace LEFT JOIN openlineage.schemas ON openlineage.schemas.dataset_id = openlineage.datasets.id AND openlineage.schemas.schema_json = tmp.dataset_schema_json WHERE (openlineage.schemas.dataset_id ISNULL AND openlineage.schemas.schema_json ISNULL) ) INSERT INTO openlineage.schemas (schema_json, dataset_id, create_at, update_at) SELECT schema_json, dataset_id, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT tmp.dataset_schema_json AS schema_json, openlineage.datasets.id AS dataset_id, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'gs://stg-lake-raw-ephemeral' AS dataset_location, 'gs://stg-lake-raw-ephemeral' AS dataset_uri, 'open_lineage_delta_input' AS dataset_name, 'gs://stg-lake-raw-ephemeral' AS dataset_namespace, '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]' AS dataset_schema_json UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]')) tmp LEFT JOIN openlineage.datasets ON openlineage.datasets.name = tmp.dataset_name AND openlineage.datasets.uri = tmp.dataset_uri AND openlineage.datasets.location = tmp.dataset_location AND openlineage.datasets.namespace = tmp.dataset_namespace LEFT JOIN openlineage.schemas ON openlineage.schemas.dataset_id = openlineage.datasets.id AND openlineage.schemas.schema_json = tmp.dataset_schema_json WHERE NOT (openlineage.schemas.dataset_id ISNULL AND openlineage.schemas.schema_json ISNULL) ) UPDATE openlineage.schemas SET update_at=CURRENT_TIMESTAMP FROM (SELECT schema_json, dataset_id FROM update_statement) AS subquery WHERE openlineage.schemas.schema_json=subquery.schema_json AND openlineage.schemas.dataset_id=subquery.dataset_id ;"


@pytest.fixture()
def fields_query_input_output():
    return "WITH insert_statement AS (SELECT openlineage.schemas.id AS schema_id, tmp.dataset_schema_field_name AS name, tmp.dataset_schema_field_type AS TYPE, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'gs://stg-lake-raw-ephemeral' AS dataset_location, 'gs://stg-lake-raw-ephemeral' AS dataset_uri, 'open_lineage_delta_input' AS dataset_name, 'gs://stg-lake-raw-ephemeral' AS dataset_namespace, '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]' AS dataset_schema_json, 'ano' AS dataset_schema_field_name, 'long' AS dataset_schema_field_type UNION (SELECT 'gs://stg-lake-raw-ephemeral', 'gs://stg-lake-raw-ephemeral', 'open_lineage_delta_input', 'gs://stg-lake-raw-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]', 'inflacao', 'double') UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]', 'ano', 'long') UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]', 'inflacao', 'double')) tmp LEFT JOIN openlineage.datasets ON openlineage.datasets.name = tmp.dataset_name AND openlineage.datasets.uri = tmp.dataset_uri AND openlineage.datasets.location = tmp.dataset_location AND openlineage.datasets.namespace = tmp.dataset_namespace LEFT JOIN openlineage.schemas ON openlineage.schemas.dataset_id = openlineage.datasets.id AND openlineage.schemas.schema_json = tmp.dataset_schema_json LEFT JOIN openlineage.fields ON openlineage.fields.schema_id = openlineage.schemas.id AND openlineage.fields.name = tmp.dataset_schema_field_name AND openlineage.fields.type = tmp.dataset_schema_field_type WHERE (openlineage.fields.schema_id ISNULL AND openlineage.fields.name ISNULL AND openlineage.fields.type ISNULL) ) INSERT INTO openlineage.fields (schema_id, name, TYPE, create_at, update_at) SELECT schema_id, name, TYPE, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT openlineage.schemas.id AS schema_id, tmp.dataset_schema_field_name AS name, tmp.dataset_schema_field_type AS TYPE, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT 'gs://stg-lake-raw-ephemeral' AS dataset_location, 'gs://stg-lake-raw-ephemeral' AS dataset_uri, 'open_lineage_delta_input' AS dataset_name, 'gs://stg-lake-raw-ephemeral' AS dataset_namespace, '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]' AS dataset_schema_json, 'ano' AS dataset_schema_field_name, 'long' AS dataset_schema_field_type UNION (SELECT 'gs://stg-lake-raw-ephemeral', 'gs://stg-lake-raw-ephemeral', 'open_lineage_delta_input', 'gs://stg-lake-raw-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]', 'inflacao', 'double') UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]', 'ano', 'long') UNION (SELECT 'gs://stg-lake-refined-ephemeral', 'gs://stg-lake-refined-ephemeral', 'open_lineage_versions', 'gs://stg-lake-refined-ephemeral', '[{''name'': ''ano'', ''type'': ''long''}, {''name'': ''inflacao'', ''type'': ''double''}]', 'inflacao', 'double')) tmp LEFT JOIN openlineage.datasets ON openlineage.datasets.name = tmp.dataset_name AND openlineage.datasets.uri = tmp.dataset_uri AND openlineage.datasets.location = tmp.dataset_location AND openlineage.datasets.namespace = tmp.dataset_namespace LEFT JOIN openlineage.schemas ON openlineage.schemas.dataset_id = openlineage.datasets.id AND openlineage.schemas.schema_json = tmp.dataset_schema_json LEFT JOIN openlineage.fields ON openlineage.fields.schema_id = openlineage.schemas.id AND openlineage.fields.name = tmp.dataset_schema_field_name AND openlineage.fields.type = tmp.dataset_schema_field_type WHERE NOT (openlineage.fields.schema_id ISNULL AND openlineage.fields.name ISNULL AND openlineage.fields.type ISNULL) ) UPDATE openlineage.fields SET update_at=CURRENT_TIMESTAMP FROM (SELECT schema_id, name, TYPE FROM update_statement) AS subquery WHERE openlineage.fields.schema_id=subquery.schema_id AND openlineage.fields.name=subquery.name AND openlineage.fields.type=subquery.type ;"


@pytest.fixture()
def runs_query_input_output():
    return 'WITH insert_statement AS (SELECT tmp.spark_logical_plan AS spark_logical_plan, openlineage.tasks.id AS task_id, TO_TIMESTAMP(tmp.event_time, \'YYYY-MM-DD"T"HH24:MI:SS.MS\') AS event_time, tmp.run_uuid AS run_uuid, tmp.action_type AS action_type, datasets_input.id AS input_dataset_id, datasets_output.id AS output_dataset_id, schemas_input.id AS input_schema_id, schemas_output.id AS output_schema_id, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT \'2023-05-06T00:11:26.569Z\' AS event_time, \'s\' AS airflow_env, \'dagname\' AS dag, \'taskname\' AS task, \'execute_save_into_data_source_command\' AS action_type, \'de3ee8b8-c16a-430d-97fa-6ee57f7dabde\' AS run_uuid, \'[{}]\' AS spark_logical_plan, \'gs://stg-lake-raw-ephemeral\' AS dataset_location_input, \'gs://stg-lake-raw-ephemeral\' AS dataset_uri_input, \'open_lineage_delta_input\' AS dataset_name_input, \'gs://stg-lake-raw-ephemeral\' AS dataset_namespace_input, \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\' AS dataset_schema_json_input, \'gs://stg-lake-refined-ephemeral\' AS dataset_location_output, \'gs://stg-lake-refined-ephemeral\' AS dataset_uri_output, \'open_lineage_versions\' AS dataset_name_output, \'gs://stg-lake-refined-ephemeral\' AS dataset_namespace_output, \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\' AS dataset_schema_json_output) tmp LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env LEFT JOIN openlineage.tasks ON openlineage.tasks.name = tmp.task AND openlineage.tasks.dag_id = openlineage.dags.id LEFT JOIN openlineage.datasets AS datasets_input ON datasets_input.name = tmp.dataset_name_input AND datasets_input.uri = tmp.dataset_uri_input AND datasets_input.location = tmp.dataset_location_input AND datasets_input.namespace = tmp.dataset_namespace_input LEFT JOIN openlineage.datasets AS datasets_output ON datasets_output.name = tmp.dataset_name_output AND datasets_output.uri = tmp.dataset_uri_output AND datasets_output.location = tmp.dataset_location_output AND datasets_output.namespace = tmp.dataset_namespace_output LEFT JOIN openlineage.schemas AS schemas_input ON schemas_input.dataset_id = datasets_input.id AND schemas_input.schema_json = tmp.dataset_schema_json_input LEFT JOIN openlineage.schemas AS schemas_output ON schemas_output.dataset_id = datasets_output.id AND schemas_output.schema_json = tmp.dataset_schema_json_output LEFT JOIN openlineage.runs ON openlineage.runs.spark_logical_plan = tmp.spark_logical_plan AND openlineage.runs.event_time = TO_TIMESTAMP(tmp.event_time, \'YYYY-MM-DD"T"HH24:MI:SS.MS\') AND openlineage.runs.run_uuid = tmp.run_uuid AND openlineage.runs.action_type = tmp.action_type AND openlineage.runs.task_id = openlineage.tasks.id AND openlineage.runs.input_dataset_id = datasets_input.id AND openlineage.runs.output_dataset_id = datasets_output.id AND openlineage.runs.input_schema_id = schemas_input.id AND openlineage.runs.output_schema_id = schemas_output.id WHERE (openlineage.runs.spark_logical_plan ISNULL AND openlineage.runs.task_id ISNULL AND openlineage.runs.event_time ISNULL AND openlineage.runs.run_uuid ISNULL AND openlineage.runs.action_type ISNULL AND openlineage.runs.input_dataset_id ISNULL AND openlineage.runs.output_dataset_id ISNULL AND openlineage.runs.input_schema_id ISNULL AND openlineage.runs.output_schema_id ISNULL) ) INSERT INTO openlineage.runs (spark_logical_plan, task_id, event_time, run_uuid, action_type, input_dataset_id, output_dataset_id, input_schema_id, output_schema_id, create_at, update_at) SELECT spark_logical_plan, task_id, event_time, run_uuid, action_type, input_dataset_id, output_dataset_id, input_schema_id, output_schema_id, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT tmp.spark_logical_plan AS spark_logical_plan, openlineage.tasks.id AS task_id, TO_TIMESTAMP(tmp.event_time, \'YYYY-MM-DD"T"HH24:MI:SS.MS\') AS event_time, tmp.run_uuid AS run_uuid, tmp.action_type AS action_type, datasets_input.id AS input_dataset_id, datasets_output.id AS output_dataset_id, schemas_input.id AS input_schema_id, schemas_output.id AS output_schema_id, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT \'2023-05-06T00:11:26.569Z\' AS event_time, \'s\' AS airflow_env, \'dagname\' AS dag, \'taskname\' AS task, \'execute_save_into_data_source_command\' AS action_type, \'de3ee8b8-c16a-430d-97fa-6ee57f7dabde\' AS run_uuid, \'[{}]\' AS spark_logical_plan, \'gs://stg-lake-raw-ephemeral\' AS dataset_location_input, \'gs://stg-lake-raw-ephemeral\' AS dataset_uri_input, \'open_lineage_delta_input\' AS dataset_name_input, \'gs://stg-lake-raw-ephemeral\' AS dataset_namespace_input, \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\' AS dataset_schema_json_input, \'gs://stg-lake-refined-ephemeral\' AS dataset_location_output, \'gs://stg-lake-refined-ephemeral\' AS dataset_uri_output, \'open_lineage_versions\' AS dataset_name_output, \'gs://stg-lake-refined-ephemeral\' AS dataset_namespace_output, \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\' AS dataset_schema_json_output) tmp LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env LEFT JOIN openlineage.tasks ON openlineage.tasks.name = tmp.task AND openlineage.tasks.dag_id = openlineage.dags.id LEFT JOIN openlineage.datasets AS datasets_input ON datasets_input.name = tmp.dataset_name_input AND datasets_input.uri = tmp.dataset_uri_input AND datasets_input.location = tmp.dataset_location_input AND datasets_input.namespace = tmp.dataset_namespace_input LEFT JOIN openlineage.datasets AS datasets_output ON datasets_output.name = tmp.dataset_name_output AND datasets_output.uri = tmp.dataset_uri_output AND datasets_output.location = tmp.dataset_location_output AND datasets_output.namespace = tmp.dataset_namespace_output LEFT JOIN openlineage.schemas AS schemas_input ON schemas_input.dataset_id = datasets_input.id AND schemas_input.schema_json = tmp.dataset_schema_json_input LEFT JOIN openlineage.schemas AS schemas_output ON schemas_output.dataset_id = datasets_output.id AND schemas_output.schema_json = tmp.dataset_schema_json_output LEFT JOIN openlineage.runs ON openlineage.runs.spark_logical_plan = tmp.spark_logical_plan AND openlineage.runs.event_time = TO_TIMESTAMP(tmp.event_time, \'YYYY-MM-DD"T"HH24:MI:SS.MS\') AND openlineage.runs.run_uuid = tmp.run_uuid AND openlineage.runs.action_type = tmp.action_type AND openlineage.runs.task_id = openlineage.tasks.id AND openlineage.runs.input_dataset_id = datasets_input.id AND openlineage.runs.output_dataset_id = datasets_output.id AND openlineage.runs.input_schema_id = schemas_input.id AND openlineage.runs.output_schema_id = schemas_output.id WHERE NOT (openlineage.runs.spark_logical_plan ISNULL AND openlineage.runs.task_id ISNULL AND openlineage.runs.event_time ISNULL AND openlineage.runs.run_uuid ISNULL AND openlineage.runs.action_type ISNULL AND openlineage.runs.input_dataset_id ISNULL AND openlineage.runs.output_dataset_id ISNULL AND openlineage.runs.input_schema_id ISNULL AND openlineage.runs.output_schema_id ISNULL) ) UPDATE openlineage.runs SET update_at=CURRENT_TIMESTAMP FROM (SELECT spark_logical_plan, task_id, event_time, run_uuid, action_type, input_dataset_id, output_dataset_id, input_schema_id, output_schema_id FROM update_statement) AS subquery WHERE openlineage.runs.spark_logical_plan=subquery.spark_logical_plan AND openlineage.runs.task_id=subquery.task_id AND openlineage.runs.event_time=subquery.event_time AND openlineage.runs.run_uuid=subquery.run_uuid AND openlineage.runs.action_type=subquery.action_type AND openlineage.runs.input_dataset_id=subquery.input_dataset_id AND openlineage.runs.output_dataset_id=subquery.output_dataset_id AND openlineage.runs.input_schema_id=subquery.input_schema_id AND openlineage.runs.output_schema_id=subquery.output_schema_id ;'


@pytest.fixture()
def lineage_fields_query_input_output():
    return 'WITH insert_statement AS (SELECT openlineage.runs.id AS run_id, fields_input.id AS field_input, fields_output.id AS field_output, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT \'2023-05-06T00:11:26.569Z\' AS event_time, \'s\' AS airflow_env, \'dagname\' AS dag, \'taskname\' AS task, \'execute_save_into_data_source_command\' AS action_type, \'de3ee8b8-c16a-430d-97fa-6ee57f7dabde\' AS run_uuid, \'[{}]\' AS spark_logical_plan, \'open_lineage_delta_input\' AS dataset_name_origin, \'gs://stg-lake-raw-ephemeral\' AS namespace_origin, \'gs://stg-lake-refined-ephemeral\' AS dataset_location, \'gs://stg-lake-refined-ephemeral\' AS dataset_uri, \'open_lineage_versions\' AS dataset_name, \'gs://stg-lake-refined-ephemeral\' AS dataset_namespace, \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\' AS dataset_schema_json, \'ano\' AS field_name_output, \'ano\' AS field_name_origin UNION (SELECT \'2023-05-06T00:11:26.569Z\', \'s\', \'dagname\', \'taskname\', \'execute_save_into_data_source_command\', \'de3ee8b8-c16a-430d-97fa-6ee57f7dabde\', \'[{}]\', \'open_lineage_delta_input\', \'gs://stg-lake-raw-ephemeral\', \'gs://stg-lake-refined-ephemeral\', \'gs://stg-lake-refined-ephemeral\', \'open_lineage_versions\', \'gs://stg-lake-refined-ephemeral\', \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\', \'inflacao\', \'inflacao\')) tmp LEFT JOIN openlineage.datasets AS datasets_output ON datasets_output.name = tmp.dataset_name AND datasets_output.uri = tmp.dataset_uri AND datasets_output.location = tmp.dataset_location AND datasets_output.namespace = tmp.dataset_namespace LEFT JOIN openlineage.schemas AS schemas_output ON schemas_output.dataset_id = datasets_output.id AND schemas_output.schema_json = tmp.dataset_schema_json LEFT JOIN openlineage.fields AS fields_output ON fields_output.schema_id = schemas_output.id AND fields_output.name = tmp.field_name_output LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env LEFT JOIN openlineage.tasks ON openlineage.tasks.name = tmp.task AND openlineage.tasks.dag_id = openlineage.dags.id LEFT JOIN openlineage.runs ON openlineage.runs.event_time = TO_TIMESTAMP(tmp.event_time, \'YYYY-MM-DD"T"HH24:MI:SS.MS\') AND openlineage.runs.action_type = tmp.action_type AND openlineage.runs.run_uuid = tmp.run_uuid AND openlineage.runs.spark_logical_plan = tmp.spark_logical_plan INNER JOIN openlineage.datasets AS datasets_input ON datasets_input.id = openlineage.runs.input_dataset_id AND datasets_input.name = tmp.dataset_name_origin AND datasets_input.namespace = tmp.namespace_origin LEFT JOIN openlineage.schemas AS schemas_input ON schemas_input.dataset_id = datasets_input.id AND schemas_input.id = openlineage.runs.input_schema_id LEFT JOIN openlineage.fields AS fields_input ON fields_input.schema_id = schemas_input.id AND fields_input.name = tmp.field_name_origin LEFT JOIN openlineage.lineage_fields ON openlineage.lineage_fields.run_id = openlineage.runs.id AND openlineage.lineage_fields.field_input = fields_input.id AND openlineage.lineage_fields.field_output = fields_output.id WHERE (openlineage.lineage_fields.run_id ISNULL AND openlineage.lineage_fields.field_input ISNULL AND openlineage.lineage_fields.field_output ISNULL) ) INSERT INTO openlineage.lineage_fields (run_id, field_input, field_output, create_at, update_at) SELECT run_id, field_input, field_output, create_at, update_at FROM insert_statement ; WITH update_statement AS (SELECT openlineage.runs.id AS run_id, fields_input.id AS field_input, fields_output.id AS field_output, CURRENT_TIMESTAMP AS create_at, CURRENT_TIMESTAMP AS update_at FROM (SELECT \'2023-05-06T00:11:26.569Z\' AS event_time, \'s\' AS airflow_env, \'dagname\' AS dag, \'taskname\' AS task, \'execute_save_into_data_source_command\' AS action_type, \'de3ee8b8-c16a-430d-97fa-6ee57f7dabde\' AS run_uuid, \'[{}]\' AS spark_logical_plan, \'open_lineage_delta_input\' AS dataset_name_origin, \'gs://stg-lake-raw-ephemeral\' AS namespace_origin, \'gs://stg-lake-refined-ephemeral\' AS dataset_location, \'gs://stg-lake-refined-ephemeral\' AS dataset_uri, \'open_lineage_versions\' AS dataset_name, \'gs://stg-lake-refined-ephemeral\' AS dataset_namespace, \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\' AS dataset_schema_json, \'ano\' AS field_name_output, \'ano\' AS field_name_origin UNION (SELECT \'2023-05-06T00:11:26.569Z\', \'s\', \'dagname\', \'taskname\', \'execute_save_into_data_source_command\', \'de3ee8b8-c16a-430d-97fa-6ee57f7dabde\', \'[{}]\', \'open_lineage_delta_input\', \'gs://stg-lake-raw-ephemeral\', \'gs://stg-lake-refined-ephemeral\', \'gs://stg-lake-refined-ephemeral\', \'open_lineage_versions\', \'gs://stg-lake-refined-ephemeral\', \'[{\'\'name\'\': \'\'ano\'\', \'\'type\'\': \'\'long\'\'}, {\'\'name\'\': \'\'inflacao\'\', \'\'type\'\': \'\'double\'\'}]\', \'inflacao\', \'inflacao\')) tmp LEFT JOIN openlineage.datasets AS datasets_output ON datasets_output.name = tmp.dataset_name AND datasets_output.uri = tmp.dataset_uri AND datasets_output.location = tmp.dataset_location AND datasets_output.namespace = tmp.dataset_namespace LEFT JOIN openlineage.schemas AS schemas_output ON schemas_output.dataset_id = datasets_output.id AND schemas_output.schema_json = tmp.dataset_schema_json LEFT JOIN openlineage.fields AS fields_output ON fields_output.schema_id = schemas_output.id AND fields_output.name = tmp.field_name_output LEFT JOIN openlineage.dags ON openlineage.dags.name = tmp.dag AND openlineage.dags.airflow_env = tmp.airflow_env LEFT JOIN openlineage.tasks ON openlineage.tasks.name = tmp.task AND openlineage.tasks.dag_id = openlineage.dags.id LEFT JOIN openlineage.runs ON openlineage.runs.event_time = TO_TIMESTAMP(tmp.event_time, \'YYYY-MM-DD"T"HH24:MI:SS.MS\') AND openlineage.runs.action_type = tmp.action_type AND openlineage.runs.run_uuid = tmp.run_uuid AND openlineage.runs.spark_logical_plan = tmp.spark_logical_plan INNER JOIN openlineage.datasets AS datasets_input ON datasets_input.id = openlineage.runs.input_dataset_id AND datasets_input.name = tmp.dataset_name_origin AND datasets_input.namespace = tmp.namespace_origin LEFT JOIN openlineage.schemas AS schemas_input ON schemas_input.dataset_id = datasets_input.id AND schemas_input.id = openlineage.runs.input_schema_id LEFT JOIN openlineage.fields AS fields_input ON fields_input.schema_id = schemas_input.id AND fields_input.name = tmp.field_name_origin LEFT JOIN openlineage.lineage_fields ON openlineage.lineage_fields.run_id = openlineage.runs.id AND openlineage.lineage_fields.field_input = fields_input.id AND openlineage.lineage_fields.field_output = fields_output.id WHERE NOT (openlineage.lineage_fields.run_id ISNULL AND openlineage.lineage_fields.field_input ISNULL AND openlineage.lineage_fields.field_output ISNULL) ) UPDATE openlineage.lineage_fields SET update_at=CURRENT_TIMESTAMP FROM (SELECT run_id, field_input, field_output FROM update_statement) AS subquery WHERE openlineage.lineage_fields.run_id=subquery.run_id AND openlineage.lineage_fields.field_input=subquery.field_input AND openlineage.lineage_fields.field_output=subquery.field_output ;'
