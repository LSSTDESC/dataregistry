import os
import sys

import pytest
import yaml
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

from database_test_utils import *


def _make_dummy_config(tmp_src_dir):
    """Make a example yaml configuration file to ingest with execution"""

    data = {
        "run_by": "somebody",
        "software_version": {"major": 1, "minor": 1, "patch": 0},
        "an_important_list": [1, 2, 3],
    }

    # Write the data to the YAML file
    with open(tmp_src_dir / "dummy_configuration_file.yaml", "w") as file:
        yaml.dump(data, file, default_flow_style=False)


def test_register_execution_with_config_file(dummy_file):
    """Test ingesting a configuration file with an execution entry"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    _make_dummy_config(tmp_src_dir)
    ex_id = _insert_execution_entry(
        datareg,
        "execution_with_configuration",
        "An execution with an input configuration file",
        configuration=str(tmp_src_dir / "dummy_configuration_file.yaml"),
    )

    # Query
    f = datareg.Query.gen_filter("execution.execution_id", "==", ex_id)
    results = datareg.Query.find_datasets(
        [
            "execution.execution_id",
            "execution.configuration",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert i < 1
        assert getattr(r, "execution.configuration") is not None
        assert getattr(r, "execution.execution_id") == ex_id
