import os

import pytest
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_SCHEMA_WORKING
from dataregistry.registrar.dataset_util import get_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


def test_delete_dataset_bad_entry(dummy_file):
    """Try do delete a dataset that doesn't exist"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Make sure we raise an exception trying to delete a dataset that doesn't exist
    with pytest.raises(ValueError, match="not found in"):
        datareg.Registrar.dataset._delete_by_id(10000)


@pytest.mark.parametrize(
    "is_dummy,dataset_name,delete_by_id",
    [
        (True, "dummy_dataset_to_delete_method1", True),
        (False, "real_dataset_to_delete_method1", True),
        (False, "real_directory_to_delete_method1", True),
        (True, "dummy_dataset_to_delete_method2", False),
        (False, "real_dataset_to_delete_method2", False),
        (False, "real_directory_to_delete_method2", False),
    ],
)
def test_delete_dataset_entry(dummy_file, is_dummy, dataset_name, delete_by_id):
    """
    Make a simple entry, then delete it, then check it was deleted.

    Does this for a dummy dataset and a real one.

    Tests both delete functions `delete()` and `_delete_by_id()`.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Where is the real data?
    if is_dummy:
        data_path = None
        location_type = "dummy"
    else:
        if dataset_name == "real_dataset_to_delete":
            data_path = str(tmp_src_dir / "file2.txt")
            assert os.path.isfile(data_path)
        else:
            data_path = str(tmp_src_dir / "directory1")
            assert os.path.isdir(data_path)
        location_type = "dataregistry"

    DNAME = f"DESC:datasets:{dataset_name}"
    DVERSION = "0.0.1"
    DOWNER_TYPE = "user"
    DOWNER = "delete_owner"

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        DNAME,
        DVERSION,
        location_type=location_type,
        old_location=data_path,
        owner_type=DOWNER_TYPE,
        owner=DOWNER,
    )

    # Now delete that entry
    if delete_by_id:
        datareg.Registrar.dataset._delete_by_id(d_id)
    else:
        datareg.Registrar.dataset.delete(DNAME, DVERSION, DOWNER, DOWNER_TYPE)

    # Check the entry was deleted
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.status",
            "dataset.delete_date",
            "dataset.delete_uid",
            "dataset.owner_type",
            "dataset.owner",
            "dataset.relative_path",
        ],
        [f],
    )

    assert len(results["dataset.status"]) == 1
    assert get_dataset_status(results["dataset.status"][0], "deleted")
    assert results["dataset.delete_date"][0] is not None
    assert results["dataset.delete_uid"][0] is not None

    if not is_dummy:
        # Make sure the file in the root_dir has gone
        data_path = _form_dataset_path(
            results["dataset.owner_type"][0],
            results["dataset.owner"][0],
            results["dataset.relative_path"][0],
            schema=DEFAULT_SCHEMA_WORKING,
            root_dir=str(tmp_root_dir),
        )
        if dataset_name == "real_dataset_to_delete":
            assert not os.path.isfile(data_path)
        else:
            assert not os.path.isdir(data_path)

    # Make sure we can not delete an already deleted entry.
    with pytest.raises(ValueError, match="previously been deleted"):
        if delete_by_id:
            datareg.Registrar.dataset._delete_by_id(d_id)
        else:
            datareg.Registrar.dataset.delete(DNAME, DVERSION, DOWNER, DOWNER_TYPE)
