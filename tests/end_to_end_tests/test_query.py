import pytest
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import inspect

from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE
from database_test_utils import *

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir="temp")


def test_query_return_format():
    """Test we get back correct data format from queries"""

    # Pandas DataFrame
    results = datareg.query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
        return_format="dataframe",
    )
    assert type(results) == pd.DataFrame

    # Property dictionary (each key is a property with a list for each row)
    results = datareg.query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
    )
    assert type(results) == dict


def test_query_all(dummy_file):
    """Test a query where no properties are chosen, i.e., 'return *'"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Add entry
    _NAME = "DESC:datasets:test_query_between_columns"
    _V_STRING = "0.0.1"

    e_id = _insert_execution_entry(datareg, "test_query_between_columns", "test")

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

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

def test_aggregate_datasets_count(dummy_file):
    """Test counting the number of datasets."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        _insert_dataset_entry(datareg, f"test_aggregate_datasets_count_{i}", "1.0.0")

    # Count datasets
    count = datareg.Query.aggregate_datasets("dataset_id", agg_func="count")
    assert count >= 3  # Ensure at least 3 were counted


def test_aggregate_datasets_count_with_none_column(dummy_file):
    """Test counting datasets with None column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        _insert_dataset_entry(datareg, f"test_count_none_col_{i}", "1.0.0")

    # Count datasets with None column
    count = datareg.Query.aggregate_datasets(column_name=None, agg_func="count")
    assert count >= 3


def test_aggregate_datasets_sum(dummy_file):
    """Test summing the column values."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        _insert_dataset_entry(datareg, f"test_aggregate_datasets_sum_{i}", "1.0.0")

    sum_value = datareg.Query.aggregate_datasets("dataset_id", agg_func="sum")
    assert sum_value >= 3


def test_aggregate_datasets_min(dummy_file):
    """Test finding the minimum value in a column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        dataset_id = f"test_aggregate_datasets_min_{i}"
        _insert_dataset_entry(datareg, dataset_id, "1.0.0")

    min_value = datareg.Query.aggregate_datasets("dataset_id", agg_func="min")
    assert min_value >= 0


def test_aggregate_datasets_max(dummy_file):
    """Test finding the maximum value in a column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        dataset_id = f"test_aggregate_datasets_max_{i}"
        _insert_dataset_entry(datareg, dataset_id, "1.0.0")

    max_value = datareg.Query.aggregate_datasets("dataset_id", agg_func="max")
    assert max_value >= 3


def test_aggregate_datasets_avg(dummy_file):
    """Test finding the average value in a column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        dataset_id = f"test_aggregate_datasets_avg_{i}"
        _insert_dataset_entry(datareg, dataset_id, "1.0.0")

    avg_value = datareg.Query.aggregate_datasets("dataset_id", agg_func="avg")
    assert avg_value > 0


def test_aggregate_datasets_with_non_dataset_table(dummy_file):
    """Test counting records in non-dataset tables."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert dataset
    d_id = _insert_dataset_entry(
        datareg,
        "test_aggregate_datasets_with_non_dataset_table",
        "0.0.1",
    )

    a_id = _insert_alias_entry(
        datareg.Registrar, "test_aggregate_datasets_with_non_dataset_table_alias", d_id
    )

    count = datareg.Query.aggregate_datasets(
        column_name=None,
        agg_func="count",
        table_name="dataset_alias",
    )
    assert count >= 1


def test_aggregate_datasets_with_filters(dummy_file):
    """Test aggregation with filters applied."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets with different versions
    for i in range(3):
        _insert_dataset_entry(
            datareg, f"test_aggregate_datasets_with_filters_{i}", "12.123.111"
        )

    # Count with version filter
    f = datareg.Query.gen_filter("dataset.version_string", "==", "12.123.111")
    count = datareg.Query.aggregate_datasets(
        column_name=None, agg_func="count", filters=[f]
    )
    assert count == 3


def test_aggregate_datasets_errors(dummy_file):
    """Test error cases for the aggregation function."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Test invalid aggregation function
    with pytest.raises(ValueError, match="agg_func must be one of"):
        datareg.Query.aggregate_datasets("dataset_id", agg_func="invalid")

    # Test invalid table name
    with pytest.raises(ValueError, match="table_name must be one of"):
        datareg.Query.aggregate_datasets("dataset_id", table_name="invalid")

    # Test non-count aggregation on non-dataset table
    with pytest.raises(ValueError, match="Can only use agg_func"):
        datareg.Query.aggregate_datasets(
            "id", agg_func="sum", table_name="dataset_alias"
        )

    # Test None column with non-count aggregation
    with pytest.raises(ValueError, match="column_name cannot be None"):
        datareg.Query.aggregate_datasets(None, agg_func="sum")

    # Test non-existent column
    with pytest.raises(ValueError, match="Column.*does not exist"):
        datareg.Query.aggregate_datasets("non_existent_column", agg_func="count")

    # Test non-numeric column with numeric aggregation
    # This requires knowing a non-numeric column in your schema
    # Assuming dataset_id is non-numeric:
    with pytest.raises(ValueError, match="must be numeric"):
        datareg.Query.aggregate_datasets("description", agg_func="sum")

@pytest.mark.parametrize(
    "table,include_table,include_schema",
    [
        (None, True, False),
        (None, False, True),
        (None, False, False),
        (None, True, True),
        ("dataset", True, False),
        ("execution", False, False),
    ]
)
def test_query_get_all_columns(dummy_file,table,include_table,include_schema):
    """Test the `get_all_columns()` function in `query.py`"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    cols = datareg.Query.get_all_columns(table=table, include_table=include_table, include_schema=include_schema)

    assert len(cols) > 0

    if table is not None:
        for att in cols:
            if include_table:
                assert table in att

def test_query_get_all_tables(dummy_file):
    """Test the `get_all_tables()` function in `query.py`"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    tables = datareg.Query.get_all_tables()

    assert len(tables) > 0
