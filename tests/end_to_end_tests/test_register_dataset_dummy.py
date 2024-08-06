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

    _NAME = "DESC:datasets:my_first_dataset"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
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
            "dataset.version_suffix",
            "dataset.data_org",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == "DESC:datasets:my_first_dataset"
        assert getattr(r, "dataset.version_string") == "0.0.1"
        assert getattr(r, "dataset.version_major") == 0
        assert getattr(r, "dataset.version_minor") == 0
        assert getattr(r, "dataset.version_patch") == 1
        assert getattr(r, "dataset.owner") == os.getenv("USER")
        assert getattr(r, "dataset.owner_type") == "user"
        assert getattr(r, "dataset.description") == "This is my first DESC dataset"
        assert getattr(r, "dataset.relative_path") == f"{_NAME}_0.0.1"
        assert getattr(r, "dataset.version_suffix") == None
        assert getattr(r, "dataset.data_org") == "dummy"
        assert i < 1


def test_manual_relpath_and_vsuffix(dummy_file):
    """Test setting the relative path and version suffix manually"""

    _NAME = "DESC:datasets:my_second_dataset"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
        relative_path="my/custom/relative_path",
        version_suffix="custom_suffix",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_suffix", "dataset.relative_path"],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == _NAME
        assert getattr(r, "dataset.version_suffix") == "custom_suffix"
        assert getattr(r, "dataset.relative_path") == "my/custom/relative_path"
        assert i < 1

    # Try to bump dataset with version suffix (should fail)
    with pytest.raises(ValueError, match="Cannot bump"):
        d_id = _insert_dataset_entry(
            datareg,
            _NAME,
            "major",
        )


@pytest.mark.parametrize(
    "v_type,ans,name",
    [
        ("major", "1.0.0", "DESC:datasets:my_first_dataset"),
        ("minor", "1.1.0", "DESC:datasets:my_first_dataset"),
        ("patch", "1.1.1", "DESC:datasets:my_first_dataset"),
        ("patch", "1.1.2", "DESC:datasets:my_first_dataset"),
        ("minor", "1.2.0", "DESC:datasets:my_first_dataset"),
        ("major", "2.0.0", "DESC:datasets:my_first_dataset"),
    ],
)
def test_dataset_bumping(dummy_file, v_type, ans, name):
    """
    Test bumping a dataset and make sure the new version is correct.

    Tests bumping datasets with and without a version suffix.
    """

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

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == name
        assert getattr(r, "dataset.version_string") == ans
        assert getattr(r, "dataset.relative_path") == f"{name}_{ans}"
        assert i < 1


@pytest.mark.parametrize("owner_type", ["user", "group", "project"])
def test_dataset_owner_types(dummy_file, owner_type):
    """Test the different owner types"""

    _NAME = f"DESC:datasets:owner_type={owner_type}"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
        owner_type=owner_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.owner_type", "dataset.name"], [f], return_format="cursorresult"
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.owner_type") == owner_type
        assert getattr(r, "dataset.name") == _NAME
        assert i < 1


def test_register_dataset_with_global_owner_set(dummy_file):
    """
    Test setting the owner and owner_type globally during the database
    initialization.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        schema=SCHEMA_VERSION,
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
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.owner") == "DESC group"
        assert getattr(r, "dataset.owner_type") == "group"
        assert i < 1


def test_register_dataset_with_modified_default_execution(dummy_file):
    """
    Test modifying the datasets default execution directly when registering the
    dataset
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Register a dataset
    with pytest.raises(ValueError, match="Cannot have character"):
        d_id_1 = _insert_dataset_entry(
            datareg,
            name,
            "3.2.1",
        )
