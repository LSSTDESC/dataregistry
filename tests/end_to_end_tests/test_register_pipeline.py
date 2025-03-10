import os
import sys

import pytest
import yaml
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE

from database_test_utils import *


def _check_dataset_has_right_execution(datareg, d_id, ex_id):
    """Query dataset to make sure it was assigned correct exectuion id"""

    # Query for execution stage 2
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.execution_id",
        ],
        [f],
    )

    assert len(results["dataset.execution_id"]) == 1
    assert results["dataset.execution_id"][0] == ex_id


def _check_execution_has_correct_dependencies(datareg, ex_id, input_datasets):
    """Query execution to make sure correct dependencies were created"""

    # Query to make sure dependencies were created
    f = datareg.Query.gen_filter("dependency.execution_id", "==", ex_id)
    results = datareg.Query.find_datasets(
        [
            "dependency.execution_id",
            "dataset.dataset_id",
            "dataset.execution_id",
            "dataset.name",
        ],
        [f],
    )

    assert len(results["dataset.name"]) == len(input_datasets)
    for thisid in results["dataset.dataset_id"]:
        assert thisid in input_datasets


def test_pipeline_entry(dummy_file):
    """
    Test making multiple executions and datasets to form a pipeline.

    Also queries to make sure dependencies are made.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Execution for stage 1
    ex_id_1 = _insert_execution_entry(
        datareg, "pipeline_stage_1", "The first stage of my pipeline"
    )

    # Datasets for stage 1
    d_id_1 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:my_first_pipeline_stage1",
        "0.0.1",
        execution_id=ex_id_1,
    )
    _check_dataset_has_right_execution(datareg, d_id_1, ex_id_1)

    # Execution for stage 2
    ex_id_2 = _insert_execution_entry(
        datareg,
        "pipeline_stage_2",
        "The second stage of my pipeline",
        input_datasets=[d_id_1],
    )
    _check_execution_has_correct_dependencies(datareg, ex_id_2, [d_id_1])

    # Datasets for stage 2
    d_id_2 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:my_first_pipeline_stage2a",
        "0.0.1",
        execution_id=ex_id_2,
    )
    _check_dataset_has_right_execution(datareg, d_id_2, ex_id_2)

    d_id_3 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:my_first_pipeline_stage2b",
        "0.0.1",
        execution_id=ex_id_2,
    )
    _check_dataset_has_right_execution(datareg, d_id_3, ex_id_2)

    # Execution for stage 3
    ex_id_3 = _insert_execution_entry(
        datareg,
        "pipeline_stage_3",
        "The third stage of my pipeline",
        input_datasets=[d_id_2, d_id_3],
    )
    _check_execution_has_correct_dependencies(datareg, ex_id_3, [d_id_2, d_id_3])
