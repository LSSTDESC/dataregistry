import pytest
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE

from database_test_utils import *

import datetime


@pytest.mark.parametrize(
    "dataset_name,column,new_value,expected_year,expected_month,expected_day",
    [
        (
            "modify_dummy_dataset_1_str",
            "description",
            "new description",
            None,
            None,
            None,
        ),
        ("modify_dummy_dataset_1_int", "description", 10293912, None, None, None),
        (
            "modify_dummy_dataset_1_date",
            "creation_date",
            datetime.datetime(2023, 5, 15, 12, 30, 0),
            2023,
            5,
            15,
        ),
        (
            "modify_dummy_dataset_1_date_str",
            "creation_date",
            "2023-05-16 14:45:00",
            2023,
            5,
            16,
        ),
        (
            "modify_dummy_dataset_1_date_str2",
            "creation_date",
            "May 17, 2023",
            2023,
            5,
            17,
        ),
    ],
)
def test_modify_dataset(
    dummy_file,
    dataset_name,
    column,
    new_value,
    expected_year,
    expected_month,
    expected_day,
):
    """
    Make a dataset entry, then modify it, then check it was modified.
    """
    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:{dataset_name}",
        "0.0.1",
        location_type="dummy",
    )

    # Modify entry
    datareg.Registrar.dataset.modify(d_id, {column: new_value})

    # Query to verify the modification
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [f"dataset.{column}"],
        [f],
    )

    # For datetime fields, check the components
    if expected_year is not None:
        result_value = results[f"dataset.{column}"][0]

        # Handle Timestamp objects (from pandas) or datetime objects
        if hasattr(result_value, "year"):
            # It's a datetime-like object (Timestamp or datetime)
            assert result_value.year == expected_year
            assert result_value.month == expected_month
            assert result_value.day == expected_day
        else:
            # It's a string
            result_str = str(result_value)
            assert str(expected_year) in result_str
            assert (
                str(expected_month).zfill(2) in result_str
                or str(expected_month) in result_str
            )
            assert (
                str(expected_day).zfill(2) in result_str
                or str(expected_day) in result_str
            )
    else:
        # For non-datetime values, check exact equality
        assert str(results[f"dataset.{column}"][0]) == str(new_value)


@pytest.mark.parametrize(
    "execution_name,column,new_value",
    [
        ("modify_dummy_execution_1", "description", "New description"),
    ],
)
def test_modify_execution(dummy_file, execution_name, column, new_value):
    """
    Make a execution entry, then mofify it, then check it was modified.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Add entry
    e_id = _insert_execution_entry(
        datareg,
        execution_name,
        "a description i wish to modify",
    )

    # Modify entry
    datareg.Registrar.execution.modify(e_id, {column: new_value})

    f = datareg.Query.gen_filter("execution.execution_id", "==", e_id)
    results = datareg.Query.find_datasets(
        [f"execution.{column}"],
        [f],
    )

    assert results[f"execution.{column}"][0] == new_value


def test_modify_not_allowed(dummy_file):
    """Make a modify attempt that is not allowed"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:bad_modify",
        "0.0.1",
        location_type="dummy",
    )

    # Try to modify an column I'm not allowed to
    with pytest.raises(ValueError, match="not modifiable"):
        datareg.Registrar.dataset.modify(d_id, {"dataset_id": d_id})

    # Try to mofify a column that doesn't exist
    with pytest.raises(ValueError, match="not exist in the schema"):
        datareg.Registrar.dataset.modify(d_id, {"my_dataset_id": 10})
