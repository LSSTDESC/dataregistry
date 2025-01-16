import pytest
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import inspect

from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_SCHEMA_WORKING
from database_test_utils import *

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir="temp")


def test_query_return_format():
    """Test we get back correct data format from queries"""

    # Pandas DataFrame
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
        return_format="dataframe",
    )
    assert type(results) == pd.DataFrame

    # Property dictionary (each key is a property with a list for each row)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
    )
    assert type(results) == dict


def test_query_all(dummy_file):
    """Test a query where no properties are chosen, i.e., 'return *'"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:datasets:test_query_all",
        "0.0.1",
    )

    # `property_names=None` should return all columns
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(property_names=None, filters=[f])

    for c, v in results.items():
        assert len(v) == 1


def test_query_between_columns(dummy_file):
    """
    Make sure when querying with a filter from one table, but only returning
    columns from another table, we get the right result.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    _NAME = "DESC:datasets:test_query_between_columns"
    _V_STRING = "0.0.1"

    e_id = _insert_execution_entry(
        datareg, "test_query_between_columns", "test"
    )

    d_id = _insert_dataset_entry(datareg, _NAME, _V_STRING, execution_id=e_id)

    a_id = _insert_alias_entry(
        datareg.Registrar, "alias:test_query_between_columns", d_id
    )

    for i in range(3):
        if i == 0:
            # Query on execution, but only return dataset columns
            f = [datareg.Query.gen_filter("execution.execution_id", "==", e_id)]
        elif i == 1:
            # Query on alias, but only return dataset columns
            f = [datareg.Query.gen_filter("dataset_alias.dataset_alias_id", "==", a_id)]
        else:
            # Query on execution and alias, but only return dataset columns
            f = [
                datareg.Query.gen_filter("execution.execution_id", "==", e_id),
                datareg.Query.gen_filter("dataset_alias.dataset_alias_id", "==", a_id),
            ]

        results = datareg.Query.find_datasets(
            property_names=["dataset.name", "dataset.version_string"],
            filters=f,
        )

        assert len(results["dataset.name"]) == 1
        assert results["dataset.name"][0] == _NAME
        assert results["dataset.version_string"][0] == _V_STRING

@pytest.mark.skipif(
    datareg.db_connection._dialect == "sqlite", reason="wildcards break for sqlite"
)
@pytest.mark.parametrize(
    "op,qstr,ans,tag",
    [
        ("~=", "DESC:datasets:test_query_name_nocasewildcard*", 3, "nocasewildcard"),
        ("==", "DESC:datasets:test_query_name_exactmatch_first", 1, "exactmatch"),
        ("~==", "DESC:datasets:Test_Query_Name_nocasewildcard*", 0, "casewildcardfail"),
        ("~==", "DESC:datasets:test_query_name_nocasewildcard*", 3, "casewildcardpass"),
    ],
)
def test_query_name(dummy_file, op, qstr, ans, tag):
    """Test a quering on a partial name with wildcards"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Add entry
    for tmp_tag in ["first", "second", "third"]:
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_query_name_{tag}_{tmp_tag}",
            "0.0.1",
        )

    # Do a wildcard search on the name
    f = datareg.Query.gen_filter("dataset.name", op, qstr)
    results = datareg.Query.find_datasets(property_names=None, filters=[f])

    # How many datasets did we find
    if ans == 0:
        assert len(results) == 0
    else:
        assert len(results) > 0
        for c, v in results.items():
            assert len(v) == ans
