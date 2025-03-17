import shlex

import dataregistry_cli.cli as cli
import pytest
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE

from database_test_utils import dummy_file
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status


def test_simple_query(dummy_file):
    """Make a simple entry, and make sure the query returns the correct result"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset 0.0.1 --location_type dummy"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Update the registered dataset
    cmd = "register dataset my_cli_dataset patch --location_type dummy"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
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
    cmd += " --creation_date '2020-01-01'"
    cmd += " --input_datasets 1 2 --execution_name 'I have given the execution a name'"
    cmd += " --is_overwritable"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
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

    datareg = DataRegistry(root_dir=str(tmp_root_dir))

    if datareg.Query._dialect != "sqlite":
        # Register a dataset
        cmd = "register dataset my_production_cli_dataset 0.1.2 --location_type dummy"
        cmd += " --owner_type production --owner production"
        cmd += f" --entry_mode production --root_dir {str(tmp_root_dir)}"
        cli.main(shlex.split(cmd))

        # Check
        f = datareg.Query.gen_filter("dataset.name", "==", "my_production_cli_dataset")
        results = datareg.Query.find_datasets(
            ["dataset.name", "dataset.version_string"], [f]
        )
        assert len(results["dataset.name"]) == 1, "Bad result from query dcli3"
        assert results["dataset.version_string"][0] == "0.1.2"


def test_delete_dataset_by_id(dummy_file, monkeypatch):
    """Make a simple entry, then delete it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset_to_delete 0.0.1 --location_type dummy"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Find the dataset id
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_delete")
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    assert len(results["dataset.dataset_id"]) == 1, "Bad result from query dcli4"
    d_id = results["dataset.dataset_id"][0]

    # Delete the dataset
    cmd = f"delete dataset_by_id {d_id}"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    monkeypatch.setattr("builtins.input", lambda _: "y")
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_delete")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.delete_date",
            "dataset.delete_uid",
            "dataset.status",
        ],
        [f],
    )

    assert len(results["dataset.dataset_id"]) == 1
    assert get_dataset_status(results["dataset.status"][0], "deleted")
    assert results["dataset.delete_date"][0] is not None
    assert results["dataset.delete_uid"][0] is not None


def test_delete_dataset_by_name(dummy_file, monkeypatch):
    """Make a simple entry, then delete it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    DNAME = "my_cli_dataset_to_delete2"
    DVERSION = "0.0.1"
    DOWNER = "delete_owner"
    DOWNER_TYPE = "user"

    # Register a dataset
    cmd = f"register dataset {DNAME} {DVERSION} --location_type dummy"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cmd += f" --owner {DOWNER} --owner_type {DOWNER_TYPE}"
    monkeypatch.setattr("builtins.input", lambda _: "y")
    cli.main(shlex.split(cmd))

    # Find the dataset id
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", DNAME)
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    assert len(results["dataset.dataset_id"]) == 1, "Bad result from query dcli4"
    d_id = results["dataset.dataset_id"][0]

    # Delete the dataset
    cmd = f"delete dataset {DNAME} {DVERSION} {DOWNER} {DOWNER_TYPE}"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", DNAME)
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.delete_date",
            "dataset.delete_uid",
            "dataset.status",
        ],
        [f],
    )

    assert len(results["dataset.dataset_id"]) == 1
    assert get_dataset_status(results["dataset.status"][0], "deleted")
    assert results["dataset.delete_date"][0] is not None
    assert results["dataset.delete_uid"][0] is not None


def test_dataset_entry_with_keywords(dummy_file):
    """Make a dataset with some keywords tagged"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset with many options
    cmd = "register dataset my_cli_dataset_keywords 1.0.0 --location_type dummy"
    cmd += " --is_overwritable --keywords simulation observation"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_keywords")
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "keyword.keyword",
        ],
        [f],
    )

    assert len(results["dataset.name"]) == 2
    for i in range(2):
        assert results["dataset.name"][i] == "my_cli_dataset_keywords"
        assert results["keyword.keyword"][i] in ["observation", "simulation"]


def test_modify_dataset(dummy_file):
    """Make a simple entry, then modify it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset_to_modify 0.0.1 --location_type dummy"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Find the dataset id
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_modify")
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    assert len(results["dataset.dataset_id"]) == 1, "Bad result from query dcli5"
    d_id = results["dataset.dataset_id"][0]

    # Modify dataset
    cmd = f"modify dataset {d_id} description 'Updated CLI desc'"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_to_modify")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.description",
        ],
        [f],
    )

    assert len(results["dataset.dataset_id"]) == 1
    assert results["dataset.description"][0] == "Updated CLI desc"


def test_modify_dataset_creation_date(dummy_file):
    """Make a simple entry, then modify its creation_date"""
    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file

    # Register a dataset
    cmd = "register dataset my_cli_dataset_date_modify 0.0.1 --location_type dummy"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Find the dataset id
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_date_modify")
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    assert len(results["dataset.dataset_id"]) == 1, "Bad result from query"
    d_id = results["dataset.dataset_id"][0]

    # Define a new date string in ISO format
    new_date = "2023-05-15T10:30:00"

    # Modify dataset creation_date
    cmd = f"modify dataset {d_id} creation_date '{new_date}'"
    cmd += f" --namespace {DEFAULT_NAMESPACE} --root_dir {str(tmp_root_dir)}"
    cli.main(shlex.split(cmd))

    # Check the modification
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset_date_modify")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.creation_date",
        ],
        [f],
    )

    assert len(results["dataset.dataset_id"]) == 1

    # Get the creation_date from results
    result_date = results["dataset.creation_date"][0]

    # Check if it's a datetime/timestamp object with attributes
    if hasattr(result_date, "year"):
        assert result_date.year == 2023
        assert result_date.month == 5
        assert result_date.day == 15
        assert result_date.hour == 10
        assert result_date.minute == 30
    else:
        # If it's returned as a string, check for the date components
        result_str = str(result_date)
        assert "2023" in result_str
        assert "05" in result_str or "5" in result_str
        assert "15" in result_str
