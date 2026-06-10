import pandas as pd
import pytest
from database_test_utils import (
    _insert_alias_entry,
    _insert_dataset_entry,
    _insert_execution_entry,
    dummy_file,  # noqa
)

from dataregistry import DataRegistry
from dataregistry.exceptions import DataRegistryColumnSpec
from dataregistry.schema import DEFAULT_NAMESPACE

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir="temp")


def test_query_return_format():
    """Test we get back correct data format from queries"""

    # Pandas DataFrame
    results = datareg.find_datasets(
        property_names=[
            "dataset.name",
            "dataset.version_string",
            "dataset.relative_path",
        ],
        filters=[],
        return_format="dataframe",
    )
    assert type(results) is pd.DataFrame

    # Property dictionary (each key is a property with a list for each row)
    results = datareg.find_datasets(
        property_names=[
            "dataset.name",
            "dataset.version_string",
            "dataset.relative_path",
        ],
        filters=[],
    )
    assert type(results) is dict


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
    f = datareg.query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.find_datasets(property_names=None, filters=[f])

    for c, v in results.items():
        assert len(v) == 1


@pytest.mark.parametrize(
    "op,offset_from_first,expected_count",
    [
        (">=", 0, 5),  # all five inserted ids
        (">", 0, 4),  # all but the first
        ("<=", 4, 5),  # everything up to and including the last
        ("<", 4, 4),  # everything up to but not including the last
        (">=", 2, 3),  # mid-range
        ("<", 2, 2),  # mid-range
    ],
)
def test_query_dataset_id_comparison(dummy_file, op, offset_from_first, expected_count):
    """
    Filtering on the integer dataset.dataset_id with ordering operators
    (>=, >, <=, <) must work. Bug report: these operators currently raise
    "Cannot apply ...", because the orderable-type check inspects the Column
    object rather than the column's underlying SQL type.
    """

    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert 5 datasets so we get 5 consecutive dataset_ids.
    ids = []
    for i in range(5):
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_query_dataset_id_comparison_{op}_{offset_from_first}_{i}",
            "0.0.1",
        )
        ids.append(d_id)

    # Anchor the filter at one of the inserted ids so other tests can't shift
    # what we count.
    pivot = ids[offset_from_first]
    f_pivot = datareg.query.gen_filter("dataset.dataset_id", op, pivot)

    # Restrict to the ids we just created so a polluted DB doesn't break us.
    f_lower = datareg.query.gen_filter("dataset.dataset_id", ">=", ids[0])
    f_upper = datareg.query.gen_filter("dataset.dataset_id", "<=", ids[-1])

    results = datareg.query.find_datasets(
        property_names=["dataset.dataset_id"],
        filters=[f_pivot, f_lower, f_upper],
    )

    assert len(results["dataset.dataset_id"]) == expected_count


@pytest.mark.parametrize(
    "column_spec,expected",
    [
        ("owner", "success"),
        ("access_api", "failure"),
        ("dataset.access_api", "success")
    ],
)
def test_query_column_spec(dummy_file, column_spec, expected):
    """
    Check that column specifications are handled correctly.  An unqualified
    column name should be rejected if it appears in more than one
    table
    """

    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    # Insert a dataset
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_query_column_spec_{column_spec}_{expected}",
        "0.0.1",
    )

    # Make a query using the column_spec
    id_filter = datareg.query.gen_filter("dataset.dataset_id", "=", d_id)

    status = "unknown"
    try:
        results = datareg.find_dataset(
            property_names=[column_spec],
            filters=[id_filter])
        status = "success"
    except DataRegistryColumnSpec:
        status = "failure"

    assert status == expected


def test_query_version_major_comparison(dummy_file):
    """
    Same check as test_query_dataset_id_comparison but on a different integer
    column (dataset.version_major), to confirm the orderable-type check works
    for any integer column, not just the primary key.
    """

    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Three datasets with distinct major versions.
    for v in ["1.0.0", "2.0.0", "3.0.0"]:
        _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_query_version_major_comparison_{v}",
            v,
        )

    # version_major >= 2 should match the 2.x and 3.x rows we just created.
    f_ge = datareg.query.gen_filter("dataset.version_major", ">=", 2)
    f_name = datareg.query.gen_filter(
        "dataset.name",
        "~==",
        "DESC:datasets:test_query_version_major_comparison_*",
    )
    results = datareg.query.find_datasets(
        property_names=["dataset.version_major"],
        filters=[f_ge, f_name],
    )

    # Skip on sqlite because the name wildcard filter doesn't work there
    # (mirroring the pattern in test_query_name).
    if datareg.db_connection._dialect != "sqlite":
        assert len(results["dataset.version_major"]) == 2
        assert sorted(results["dataset.version_major"]) == [2, 3]


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
            f = [datareg.query.gen_filter("execution.execution_id", "==", e_id)]
        elif i == 1:
            # Query on alias, but only return dataset columns
            f = [datareg.query.gen_filter("dataset_alias.dataset_alias_id", "==", a_id)]
        else:
            # Query on execution and alias, but only return dataset columns
            f = [
                datareg.query.gen_filter("execution.execution_id", "==", e_id),
                datareg.query.gen_filter("dataset_alias.dataset_alias_id", "==", a_id),
            ]

        results = datareg.query.find_datasets(
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
    f = datareg.query.gen_filter("dataset.name", op, qstr)
    results = datareg.find_datasets(property_names=None, filters=[f])

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
    count = datareg.query.aggregate_datasets("dataset_id", agg_func="count")
    assert count >= 3  # Ensure at least 3 were counted


def test_aggregate_datasets_count_with_none_column(dummy_file):
    """Test counting datasets with None column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        _insert_dataset_entry(datareg, f"test_count_none_col_{i}", "1.0.0")

    # Count datasets with None column
    count = datareg.query.aggregate_datasets(column_name=None, agg_func="count")
    assert count >= 3


def test_aggregate_datasets_sum(dummy_file):
    """Test summing the column values."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        _insert_dataset_entry(datareg, f"test_aggregate_datasets_sum_{i}", "1.0.0")

    sum_value = datareg.query.aggregate_datasets("dataset_id", agg_func="sum")
    assert sum_value >= 3


def test_aggregate_datasets_min(dummy_file):
    """Test finding the minimum value in a column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        dataset_id = f"test_aggregate_datasets_min_{i}"
        _insert_dataset_entry(datareg, dataset_id, "1.0.0")

    min_value = datareg.query.aggregate_datasets("dataset_id", agg_func="min")
    assert min_value >= 0


def test_aggregate_datasets_max(dummy_file):
    """Test finding the maximum value in a column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        dataset_id = f"test_aggregate_datasets_max_{i}"
        _insert_dataset_entry(datareg, dataset_id, "1.0.0")

    max_value = datareg.query.aggregate_datasets("dataset_id", agg_func="max")
    assert max_value >= 3


def test_aggregate_datasets_avg(dummy_file):
    """Test finding the average value in a column."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Insert datasets
    for i in range(3):
        dataset_id = f"test_aggregate_datasets_avg_{i}"
        _insert_dataset_entry(datareg, dataset_id, "1.0.0")

    avg_value = datareg.query.aggregate_datasets("dataset_id", agg_func="avg")
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

    count = datareg.query.aggregate_datasets(
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
    f = datareg.query.gen_filter("dataset.version_string", "==", "12.123.111")
    count = datareg.query.aggregate_datasets(
        column_name=None, agg_func="count", filters=[f]
    )
    assert count == 3


def test_aggregate_datasets_errors(dummy_file):
    """Test error cases for the aggregation function."""
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Test invalid aggregation function
    with pytest.raises(ValueError, match="agg_func must be one of"):
        datareg.query.aggregate_datasets("dataset_id", agg_func="invalid")

    # Test invalid table name
    with pytest.raises(ValueError, match="table_name must be one of"):
        datareg.query.aggregate_datasets("dataset_id", table_name="invalid")

    # Test non-count aggregation on non-dataset table
    with pytest.raises(ValueError, match="Can only use agg_func"):
        datareg.query.aggregate_datasets(
            "id", agg_func="sum", table_name="dataset_alias"
        )

    # Test None column with non-count aggregation
    with pytest.raises(ValueError, match="column_name cannot be None"):
        datareg.query.aggregate_datasets(None, agg_func="sum")

    # Test non-existent column
    with pytest.raises(ValueError, match="Column.*does not exist"):
        datareg.query.aggregate_datasets("non_existent_column", agg_func="count")

    # Test non-numeric column with numeric aggregation
    # This requires knowing a non-numeric column in your schema
    # Assuming dataset_id is non-numeric:
    with pytest.raises(ValueError, match="must be numeric"):
        datareg.query.aggregate_datasets("description", agg_func="sum")


@pytest.mark.parametrize(
    "table,include_table,include_schema",
    [
        (None, True, False),
        (None, False, True),
        (None, False, False),
        (None, True, True),
        ("dataset", True, False),
        ("execution", False, False),
    ],
)
def test_query_get_all_columns(dummy_file, table, include_table, include_schema):
    """Test the `get_all_columns()` function in `query.py`"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    cols = datareg.query.get_all_columns(
        table=table, include_table=include_table, include_schema=include_schema
    )

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

    tables = datareg.get_all_tables()

    assert len(tables) > 0
