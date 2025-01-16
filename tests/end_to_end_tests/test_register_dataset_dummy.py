import os
import sys

import pandas as pd
import pytest
import sqlalchemy
import yaml
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_SCHEMA_WORKING
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


def test_register_dataset_defaults(dummy_file):
    """
    Make a simple entry, and make sure the query returns the correct results
    for default values
    """

    _NAME = "DESC:datasets:test_register_dataset_defaults"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(None, [f], strip_table_names=True)

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["name"]) == 1

    # Check the result
    assert results["name"][0] == _NAME
    assert results["version_string"][0] == "0.0.1"
    assert results["version_major"][0] == 0
    assert results["version_minor"][0] == 0
    assert results["version_patch"][0] == 1
    assert results["owner"][0] == os.getenv("USER")
    assert results["owner_type"][0] == "user"
    assert results["description"][0] == None
    assert results["relative_path"][0] == f".gen_paths/{_NAME}_0.0.1"
    assert results["data_org"][0] == "dummy"
    assert results["execution_id"][0] >= 0
    assert results["dataset_id"][0] >= 0
    assert results["creation_date"][0] is None
    assert results["register_date"][0] is not None
    assert results["creator_uid"][0] == os.getenv("USER")
    assert results["access_api"][0] is None
    assert results["access_api_configuration"][0] is None
    assert results["nfiles"][0] == 0
    assert results["total_disk_space"][0] == 0
    assert results["register_root_dir"][0] == str(tmp_root_dir)
    assert get_dataset_status(results["status"][0], "replaced") == False
    assert results["is_overwritable"][0] == False
    assert results["status"][0] == 1
    assert results["archive_date"][0] is None
    assert results["archive_path"][0] is None
    assert results["delete_date"][0] is None
    assert results["delete_uid"][0] is None
    assert results["move_date"][0] is None
    assert results["location_type"][0] == "dummy"
    assert results["url"][0] is None
    assert results["contact_email"][0] is None
    assert results["replace_id"][0] is None
    assert results["replace_iteration"][0] == 0


def test_register_dataset_manual(dummy_file):
    """
    Make a simple entry, and make sure the query returns the correct results
    for manually selected values
    """

    _NAME = "DESC:datasets:test_register_dataset_manual"
    _DESCRIPTION = "A manual description"
    _OWNER = "test_owner"
    _OWNER_TYPE = "group"
    _REL_PATH = "manual/rel/path"
    _ACCESS_API = "test_api"
    _IS_OVERWRITABLE = True

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
        description=_DESCRIPTION,
        owner=_OWNER,
        owner_type=_OWNER_TYPE,
        relative_path=_REL_PATH,
        access_api=_ACCESS_API,
        is_overwritable=_IS_OVERWRITABLE,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(None, [f], strip_table_names=True)

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["name"]) == 1

    # Check the result
    assert results["name"][0] == _NAME
    assert results["version_string"][0] == "0.0.1"
    assert results["version_major"][0] == 0
    assert results["version_minor"][0] == 0
    assert results["version_patch"][0] == 1
    assert results["owner"][0] == _OWNER
    assert results["owner_type"][0] == _OWNER_TYPE
    assert results["description"][0] == _DESCRIPTION
    assert results["relative_path"][0] == _REL_PATH
    assert results["data_org"][0] == "dummy"
    assert results["execution_id"][0] >= 0
    assert results["dataset_id"][0] >= 0
    assert results["creation_date"][0] is None
    assert results["register_date"][0] is not None
    assert results["creator_uid"][0] == os.getenv("USER")
    assert results["access_api"][0] == _ACCESS_API
    assert results["access_api_configuration"][0] is None
    assert results["nfiles"][0] == 0
    assert results["total_disk_space"][0] == 0
    assert results["register_root_dir"][0] == str(tmp_root_dir)
    assert get_dataset_status(results["status"][0], "replaced") == False
    assert results["is_overwritable"][0] == _IS_OVERWRITABLE
    assert results["status"][0] == 1
    assert results["archive_date"][0] is None
    assert results["archive_path"][0] is None
    assert results["delete_date"][0] is None
    assert results["delete_uid"][0] is None
    assert results["move_date"][0] is None
    assert results["location_type"][0] == "dummy"
    assert results["url"][0] is None
    assert results["contact_email"][0] is None
    assert results["replace_id"][0] is None
    assert results["replace_iteration"][0] == 0


@pytest.mark.parametrize(
    "v_type,ans",
    [
        ("major", "1.0.0"),
        ("minor", "1.1.0"),
        ("patch", "1.1.1"),
        ("patch", "1.1.2"),
        ("minor", "1.2.0"),
        ("major", "2.0.0"),
    ],
)
def test_dataset_bumping(dummy_file, v_type, ans):
    """Test bumping a dataset and make sure the new version is correct"""

    _NAME = "DESC:datasets:test_register_dataset_defaults"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        v_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [f],
    )

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["dataset.name"]) == 1

    # Check the result
    assert results["dataset.name"][0] == _NAME
    assert results["dataset.version_string"][0] == ans
    assert results["dataset.relative_path"][0] == f".gen_paths/{_NAME}_{ans}"


@pytest.mark.parametrize("owner_type", ["user", "group", "project"])
def test_dataset_owner_types(dummy_file, owner_type):
    """Test the different owner types"""

    _NAME = f"DESC:datasets:owner_type={owner_type}"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
        owner_type=owner_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(["dataset.owner_type", "dataset.name"], [f])

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["dataset.name"]) == 1

    # Check the result
    assert results["dataset.owner_type"][0] == owner_type
    assert results["dataset.name"][0] == _NAME


def test_register_dataset_with_global_owner_set(dummy_file):
    """
    Test setting the owner and owner_type globally during the database
    initialization.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        schema=DEFAULT_SCHEMA_WORKING,
        owner="DESC group",
        owner_type="group",
    )

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:datasets:global_user_dataset",
        "0.0.1",
        owner=None,
        owner_type=None,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.owner",
            "dataset.owner_type",
        ],
        [f],
    )

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["dataset.owner"]) == 1

    # Check the result
    assert results["dataset.owner_type"][0] == "group"
    assert results["dataset.owner"][0] == "DESC group"


def test_register_dataset_with_modified_default_execution(dummy_file):
    """
    Test modifying the datasets default execution directly when registering the
    dataset
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    d_id_1 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:execution_test",
        "0.0.1",
    )

    d_id_2 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:execution_test_2",
        "0.0.1",
        execution_name="Overwrite execution auto name",
        execution_description="Overwrite execution auto description",
        execution_site="TestMachine",
        input_datasets=[d_id_1],
    )

    # Query on execution
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id_2)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "execution.execution_id",
            "execution.description",
            "execution.site",
            "execution.name",
        ],
        [f],
    )

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["dataset.name"]) == 1

    # Check the result
    assert results["execution.name"][0] == "Overwrite execution auto name"
    assert results["execution.description"][0] == "Overwrite execution auto description"
    assert results["execution.site"][0] == "TestMachine"
    ex_id_1 = results["execution.execution_id"][0]

    # Query on dependency
    f = datareg.Query.gen_filter("dependency.input_id", "==", d_id_1)
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dependency.execution_id",
            "dependency.input_id",
        ],
        [f],
    )

    # First make sure we find a result
    assert len(results) > 0
    assert len(results["dataset.dataset_id"]) == 1

    # Check the result
    assert results["dependency.execution_id"][0] == ex_id_1


@pytest.mark.parametrize(
    "return_format_str,expected_type",
    [
        ("dataframe", pd.DataFrame),
        ("property_dict", dict),
    ],
)
def test_dataset_query_return_format(dummy_file, return_format_str, expected_type):
    """Test we get back correct data format from dataset queries"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    _NAME = f"DESC:datasets:query_return_test_{return_format_str}"

    # Register a dataset
    d_id_1 = _insert_dataset_entry(
        datareg,
        _NAME,
        "3.2.1",
    )

    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id_1)

    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [f],
        return_format=return_format_str,
    )
    assert type(results) == expected_type


def test_query_all(dummy_file):
    """Test a query where no properties are chosen, i.e., 'return *'"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Register a dataset
    d_id_1 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:test_return_all",
        "3.2.1",
    )

    results = datareg.Query.find_datasets()

    assert results is not None


@pytest.mark.parametrize(
    "name",
    ["bad/name", "bad name", "b$dname", "*adname", "b&dname", "badname?", "bad\\name"],
)
def test_dataset_bad_name_string(dummy_file, name):
    """Make sure illegal names get caught"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Register a dataset
    with pytest.raises(ValueError, match="Cannot have character"):
        d_id_1 = _insert_dataset_entry(
            datareg,
            name,
            "3.2.1",
        )
