import os
import pandas as pd
import sqlalchemy

from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir="temp_root_dir")


def test_cli_basic_dataset():
    """Test queries for the dataset table entered from the CLI script"""

    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    assert len(results["dataset.name"]) == 2, "Bad result from query dcli1"


def test_cli_dataset_with_execution():
    """Test executions entries using CLI"""

    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset3")
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.relative_path",
            "execution.name",
            "execution.execution_id",
            "dataset.is_overwritable",
        ],
        [f],
    )
    assert len(results["dataset.name"]) == 1, "Bad result from query dcli2"
    assert results["execution.name"][0] == "I have given the execution a name"
    assert results["dataset.is_overwritable"][0] == True


def test_cli_production_entry():
    """Test the production entry from the CLI"""

    if datareg.Query._dialect != "sqlite":
        # Establish connection to database (production schema)
        datareg_prod = DataRegistry(schema="production", root_dir="temp_root_dir")

        f = datareg_prod.Query.gen_filter(
            "dataset.name", "==", "my_production_cli_dataset"
        )
        results = datareg_prod.Query.find_datasets(
            ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
        )
        assert len(results["dataset.name"]) == 1, "Bad result from query dcli3"
        assert results["dataset.version_string"][0] == "0.1.2"
