import shlex

import dataregistry_cli.cli as cli
import pytest
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

from database_test_utils import dummy_file
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status


def test_simple_query(dummy_file):
    """Make a simple entry, and make sure the query returns the correct result"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset 0.0.1 --location_type dummy"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Update the registered dataset
    cmd = "register dataset my_cli_dataset patch --location_type dummy"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string"], [f]
    )
    assert len(results["dataset.name"]) == 2, "Bad result from query dcli1"


def test_dataset_entry_with_execution(dummy_file):
    """Make a dataset and execution together"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset with many options
    cmd = "register dataset my_cli_dataset3 1.2.3 --location_type dummy"
    cmd += " --description 'This is my dataset description'"
    cmd += " --access_api 'Awesome API' --owner DESC --owner_type group"
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
        cmd = "register dataset my_production_cli_dataset 0.1.2 --location_type dummy"
        cmd += " --owner_type production --owner production"
        cmd += f" --schema production --root_dir {str(tmp_root_dir)}"
        cli.main(shlex.split(cmd))

        # Check
        f = datareg.Query.gen_filter("dataset.name", "==", "my_production_cli_dataset")
        results = datareg.Query.find_datasets(
            ["dataset.name", "dataset.version_string"], [f]
        )
        assert len(results["dataset.name"]) == 1, "Bad result from query dcli3"
        assert results["dataset.version_string"][0] == "0.1.2"


def test_delete_dataset(dummy_file):
    """Make a simple entry, then delete it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset_to_delete 0.0.1 --location_type dummy"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Find the dataset id
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_delete")
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    assert len(results["dataset.dataset_id"]) == 1, "Bad result from query dcli4"
    d_id = results["dataset.dataset_id"][0]

    # Delete the dataset
    cmd = f"delete dataset {d_id}"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_delete")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.delete_date",
            "dataset.delete_uid",
            "dataset.status",
        ],
        [f],
        return_format="cursorresult",
    )
    for r in results:
        assert get_dataset_status(getattr(r, "dataset.status"), "deleted")
        assert getattr(r, "dataset.delete_date") is not None
        assert getattr(r, "dataset.delete_uid") is not None


def test_dataset_entry_with_keywords(dummy_file):
    """Make a dataset with some keywords tagged"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset with many options
    cmd = "register dataset my_cli_dataset_keywords 1.0.0 --location_type dummy"
    cmd += " --is_overwritable --keywords simulation observation"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_keywords")
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "keyword.keyword",
        ],
        [f],
        return_format="cursorresult",
    )
    for r in results:
        assert getattr(r, "dataset.name") == "my_cli_dataset_keywords"
        assert getattr(r, "keyword.keyword") in ["observation", "simulation"]

def test_modify_dataset(dummy_file):
    """Make a simple entry, then modify it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset_to_modify 0.0.1 --location_type dummy"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Find the dataset id
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_modify")
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    assert len(results["dataset.dataset_id"]) == 1, "Bad result from query dcli5"
    d_id = results["dataset.dataset_id"][0]

    # Modify dataset
    cmd = f"modify dataset {d_id} description 'Updated CLI desc'"
    cmd += f" --schema {SCHEMA_VERSION} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_modify")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.description",
        ],
        [f],
        return_format="cursorresult",
    )
    for r in results:
        assert getattr(r, "dataset.description") == "Updated CLI desc"
