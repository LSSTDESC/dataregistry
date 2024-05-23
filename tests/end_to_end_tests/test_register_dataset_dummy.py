import os
import sys

import pandas as pd
import pytest
import sqlalchemy
import yaml
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


def test_register_dataset(dummy_file):
    """Make a simple entry, and make sure the query returns the correct result"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "test_register_dataset::dummy_dataset",
        "0.0.1",
        description="This is my first DESC dataset",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.owner",
            "dataset.owner_type",
            "dataset.description",
            "dataset.version_major",
            "dataset.version_minor",
            "dataset.version_patch",
            "dataset.relative_path",
            "dataset.data_org",
        ],
        [f],
        return_format="cursorresult",
    )

    # Check
    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == "test_register_dataset::dummy_dataset"
        assert getattr(r, "dataset.version_string") == "0.0.1"
        assert getattr(r, "dataset.version_major") == 0
        assert getattr(r, "dataset.version_minor") == 0
        assert getattr(r, "dataset.version_patch") == 1
        assert getattr(r, "dataset.owner") == os.getenv("USER")
        assert getattr(r, "dataset.owner_type") == "user"
        assert getattr(r, "dataset.description") == "This is my first DESC dataset"
        assert (
            getattr(r, "dataset.relative_path")
            == "test_register_dataset::dummy_dataset_0.0.1"
        )
        assert getattr(r, "dataset.data_org") == "dummy"
        assert i < 1


@pytest.mark.parametrize(
    "owner,owner_type,relative_path",
    [
        ("DESC", "group", "ci_tests/dummy_dataset/my_second_dummy_dataset"),
    ],
)
def test_manual_relative_path(dummy_file, owner, owner_type, relative_path):
    """Test setting the relative path manually when registering a dataset"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "test_manual_relative_path::dataset",
        "0.0.1",
        relative_path=relative_path,
        owner=owner,
        owner_type=owner_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.relative_path",
            "dataset.owner",
            "dataset.owner_type",
        ],
        [f],
        return_format="cursorresult",
    )

    # Check
    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == "test_manual_relative_path::dataset"
        assert getattr(r, "dataset.relative_path") == relative_path
        assert getattr(r, "dataset.owner") == owner
        assert getattr(r, "dataset.owner_type") == owner_type
        assert i < 1


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

    # Dataset that was created in `test_register_dataset()`
    name = "test_register_dataset::dummy_dataset"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        name,
        v_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [f],
        return_format="cursorresult",
    )

    # Check
    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == name
        assert getattr(r, "dataset.version_string") == ans
        assert getattr(r, "dataset.relative_path") == f"{name}_{ans}"
        assert i < 1


def test_dataset_bumping_with_suffix(dummy_file):
    """Test bumping a dataset with version suffix, should fail"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "test_dataset_bumping_with_suffix::dataset",
        "1.0.0",
        version_suffix="v1",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.version_suffix",
        ],
        [f],
        return_format="cursorresult",
    )

    # Check
    for i, r in enumerate(results):
        assert getattr(r, "dataset.version_suffix") == "v1"
        assert i < 1

    # Try to bump it
    with pytest.raises(ValueError, match="Cannot bump"):
        _ = _insert_dataset_entry(
            datareg,
            "test_dataset_bumping_with_suffix::dataset",
            "major",
        )


@pytest.mark.parametrize("owner_type", ["user", "group", "project"])
def test_dataset_owner_types(dummy_file, owner_type):
    """Test the different owner types"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"test_dataset_owner_types::dataset_{owner_type}",
        "0.0.1",
        owner_type=owner_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.owner_type"], [f], return_format="cursorresult"
    )

    # Check
    for i, r in enumerate(results):
        assert getattr(r, "dataset.owner_type") == owner_type
        assert i < 1


@pytest.mark.parametrize(
    "owner_local,owner_type_local,owner_global,owner_type_global",
    [
        (None, None, "DESC group", "group"),
        (None, None, "DESC project", "project"),
        ("DESC local user", "user", "DESC project", "project"),
    ],
)
def test_register_dataset_with_global_owner_set(
    dummy_file, owner_local, owner_type_local, owner_global, owner_type_global
):
    """
    Test setting the owner and owner_type globally during the database
    initialization.

    If the local owner/owner_type is set, it should take priority
    """

    # Establish connection to database with preset owner/owner_type
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        schema=SCHEMA_VERSION,
        owner=owner_global,
        owner_type=owner_type_global,
    )

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"test_register_dataset_with_global_owner_set::{owner_local}_{owner_type_local}",
        "0.0.1",
        owner=owner_local,
        owner_type=owner_type_local,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.owner",
            "dataset.owner_type",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        if owner_local is not None:
            assert getattr(r, "dataset.owner") == owner_local
            assert getattr(r, "dataset.owner_type") == owner_type_local
        else:
            assert getattr(r, "dataset.owner") == owner_global
            assert getattr(r, "dataset.owner_type") == owner_type_global
        assert i < 1


def test_register_dataset_with_modified_default_execution(dummy_file):
    """
    Test modifying the datasets default execution directly when registering the
    dataset
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Create two datasets, the first feeds into the second execution
    d_id_1 = _insert_dataset_entry(
        datareg,
        "test_default_execution::input_dataset",
        "0.0.1",
    )

    d_id_2 = _insert_dataset_entry(
        datareg,
        "test_default_execution::execution_dataset",
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
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "execution.name") == "Overwrite execution auto name"
        assert (
            getattr(r, "execution.description")
            == "Overwrite execution auto description"
        )
        assert getattr(r, "execution.site") == "TestMachine"
        ex_id_1 = getattr(r, "execution.execution_id")
        assert i < 1

    # Query on dependency
    f = datareg.Query.gen_filter("dependency.input_id", "==", d_id_1)
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dependency.execution_id",
            "dependency.input_id",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dependency.execution_id") == ex_id_1
        assert i < 1


@pytest.mark.parametrize(
    "return_format_str,expected_type",
    [
        ("cursorresult", sqlalchemy.engine.cursor.CursorResult),
        ("dataframe", pd.DataFrame),
        ("property_dict", dict),
    ],
)
def test_dataset_query_return_format(dummy_file, return_format_str, expected_type):
    """Test we get back correct data format from dataset queries"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Register a dataset
    d_id_1 = _insert_dataset_entry(
        datareg,
        f"test_dataset_query_return_format::{return_format_str}",
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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Register a dataset
    d_id_1 = _insert_dataset_entry(
        datareg,
        "test_query_all::dataset",
        "3.2.1",
    )

    results = datareg.Query.find_datasets()

    assert results is not None


@pytest.mark.parametrize("relative_path", ["dummy_dataset/auto_name_dataset"])
def test_manual_relative_path_and_auto_name(dummy_file, relative_path):
    """
    Test setting the relative path manually when registering a dataset.

    In this case generate the dataset name from the relative path.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        None,
        "0.0.1",
        relative_path=relative_path,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.relative_path",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == "auto_name_dataset"
        assert getattr(r, "dataset.relative_path") == relative_path
        assert i < 1

@pytest.mark.parametrize(
    "name",
    [
        ("slashes/are/bad"),
        (r"slashes\are\bad"),
        ("questions?"),
        ("stars*"),
        ("dollar$"),
        ("amper&sand"),
    ],
)
def test_illegal_dataset_name(dummy_file,name):
    """
    Registering datasets with illegal names should fail

    Illegal names are those with characters "/\?*$&"
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry with bad name
    with pytest.raises(ValueError, match="Cannot have character"):
        d_id = _insert_dataset_entry(
            datareg,
            name,
            "1.0.0",
        )
