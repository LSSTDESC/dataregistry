import dataregistry_cli.cli as cli
from dataregistry.db_basic import SCHEMA_VERSION
from dataregistry import DataRegistry
import shlex
from test_end_to_end_python_api import dummy_file
import pytest


def test_simple_query(dummy_file):
    """Make a simple entry, and make sure the query returns the correct result"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset 0.0.1 --is_dummy"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Update the registered dataset
    cmd = "register dataset my_cli_dataset2 patch --is_dummy --name my_cli_dataset"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    assert len(results["dataset.name"]) == 2, "Bad result from query dcli1"


def test_dataset_entry_with_execution(dummy_file):
    """Make a dataset and execution together"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset with many options
    cmd = "register dataset my_cli_dataset3 1.2.3 --is_dummy"
    cmd += " --description 'This is my dataset description'"
    cmd += " --access_API 'Awesome API' --owner DESC --owner_type group"
    cmd += " --version_suffix test --creation_date '2020-01-01'"
    cmd += " --input_datasets 1 2 --execution_name 'I have given the execution a name'"
    cmd += " --is_overwritable"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
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


def test_production_entry(dummy_file):
    """Make a simple entry to the production schema and check result"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema="production")

    if datareg.Query._dialect != "sqlite":
        # Register a dataset
        cmd = "register dataset my_production_cli_dataset 0.1.2 --is_dummy"
        cmd += " --owner_type production"
        cmd += f" --schema production --root_dir {str(tmp_root_dir)}"
        cli.main(shlex.split(cmd))

        # Check
        f = datareg.Query.gen_filter("dataset.name", "==", "my_production_cli_dataset")
        results = datareg.Query.find_datasets(
            ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
        )
        assert len(results["dataset.name"]) == 1, "Bad result from query dcli3"
        assert results["dataset.version_string"][0] == "0.1.2"
