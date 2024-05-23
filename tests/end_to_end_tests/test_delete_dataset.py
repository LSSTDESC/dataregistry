import os

import pytest
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
from dataregistry.registrar.dataset_util import get_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


def test_delete_dataset_bad_entry(dummy_file):
    """Try do delete a dataset that doesn't exist"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Make sure we raise an exception trying to delete a dataset that doesn't exist
    with pytest.raises(ValueError, match="not found in"):
        datareg.Registrar.dataset.delete(10000)


@pytest.mark.parametrize(
    "is_dummy,dataset_name",
    [
        (True, "dummy_dataset_to_delete"),
        (False, "real_dataset_to_delete"),
        (False, "real_directory_to_delete"),
    ],
)
def test_delete_dataset_entry(dummy_file, is_dummy, dataset_name):
    """
    Make a simple entry, then delete it, then check it was deleted.

    Does this for a dummy dataset and a real one.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

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

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:{dataset_name}",
        "0.0.1",
        location_type=location_type,
        old_location=data_path,
    )

    # Now delete that entry
    datareg.Registrar.dataset.delete(d_id)

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
        return_format="cursorresult",
    )

    for r in results:
        assert get_dataset_status(getattr(r, "dataset.status"), "deleted")
        assert getattr(r, "dataset.delete_date") is not None
        assert getattr(r, "dataset.delete_uid") is not None

    if not is_dummy:
        # Make sure the file in the root_dir has gone
        data_path = _form_dataset_path(
            getattr(r, "dataset.owner_type"),
            getattr(r, "dataset.owner"),
            getattr(r, "dataset.relative_path"),
            schema=SCHEMA_VERSION,
            root_dir=str(tmp_root_dir),
        )
        if dataset_name == "real_dataset_to_delete":
            assert not os.path.isfile(data_path)
        else:
            assert not os.path.isdir(data_path)

    # Make sure we can not delete an already deleted entry.
    with pytest.raises(ValueError, match="has already been deleted"):
        datareg.Registrar.dataset.delete(d_id)
