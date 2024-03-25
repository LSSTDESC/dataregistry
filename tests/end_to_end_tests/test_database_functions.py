from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
import pytest

from database_test_utils import *

def test_find_entry(dummy_file):
    """
    Test the `find_entry` function.

    First create a dataset/execution/alias entry, then make sure we can find it
    using the generic `find_entry` function in the `BaseTable` class.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Make a dataset
    d_id = _insert_dataset_entry(datareg, "test_find_entry/dataset", "0.0.1")

    # Find it
    r = datareg.Registrar.dataset.find_entry(d_id)
    assert r is not None
    assert r.dataset_id == d_id
    assert r.relative_path == "test_find_entry/dataset"
    assert r.version_string == "0.0.1"

    # Make an execution
    e_id = _insert_execution_entry(datareg, "test_find_entry_execution", "an execution")

    # Find it
    r = datareg.Registrar.execution.find_entry(e_id)
    assert r is not None
    assert r.description == "an execution"
    assert r.name == "test_find_entry_execution"

    # Make a dataset alias
    da_id = _insert_alias_entry(datareg, "test_find_entry_alias", d_id)

    # Find it
    r = datareg.Registrar.dataset_alias.find_entry(da_id)
    assert r is not None
    assert r.alias == "test_find_entry_alias"
    assert r.dataset_id == d_id
