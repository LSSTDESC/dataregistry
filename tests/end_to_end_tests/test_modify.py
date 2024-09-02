import pytest
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

from database_test_utils import *


@pytest.mark.parametrize(
    "dataset_name,column,new_value",
    [
        ("modify_dummy_dataset_1_str", "description", "new description"),
        ("modify_dummy_dataset_1_int", "description", 10293912),
    ],
)
def test_modify_dataset(dummy_file, dataset_name, column, new_value):
    """
    Make a dataset entry, then mofify it, then check it was modified.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:{dataset_name}",
        "0.0.1",
        location_type="dummy",
    )

    # Modify entry
    datareg.Registrar.dataset.modify(d_id, {column: new_value})

    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [f"dataset.{column}"],
        [f],
    )

    assert results[f"dataset.{column}"][0] == str(new_value)


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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

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
