import os
import sys

import pytest
import yaml
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


@pytest.mark.parametrize("data_org", ["file", "directory"])
def test_copy_data(dummy_file, data_org):
    """Test copying real data into the registry (from an `old_location`)"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # File/directory we are copying in
    if data_org == "file":
        data_path = str(tmp_src_dir / "file1.txt")
    else:
        data_path = str(tmp_src_dir / "directory1")

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:copy_real_{data_org}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.data_org", "dataset.nfiles", "dataset.total_disk_space"],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.data_org") == data_org
        assert getattr(r, "dataset.nfiles") == 1
        assert getattr(r, "dataset.total_disk_space") > 0
        assert i < 1


@pytest.mark.parametrize(
    "data_org,data_path",
    [
        ("file", "file1.txt"),
        ("directory", "dummy_dir"),
    ],
)
def test_on_location_data(dummy_file, data_org, data_path):
    """
    Test ingesting real data into the registry (already on location).
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_on_location_data_{data_org}",
        "0.0.1",
        old_location=None,
        location_type="dataregistry",
        relative_path=data_path,
    )

    f = datareg.Query.gen_filter("dataset.relative_path", "==", data_path)
    results = datareg.Query.find_datasets(
        [
            "dataset.data_org",
            "dataset.nfiles",
            "dataset.total_disk_space",
        ],
        [f],
    )

    assert len(results["dataset.data_org"]) == 1

    assert results["dataset.data_org"][0] == data_org
    assert results["dataset.nfiles"][0] == 1
    assert results["dataset.total_disk_space"][0] > 0

@pytest.mark.parametrize("link", ["file1_sym.txt", "directory1_sym"])
def test_registering_symlinks(dummy_file, link):
    """
    The dataregistry does not allow registration through symlinks, make sure
    this raises an error.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    data_path = str(tmp_src_dir / link)

    with pytest.raises(ValueError, match="does not support symlinks"):
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_register_symlink_{link}",
            "0.0.1",
            old_location=data_path,
            location_type="dataregistry",
        )


@pytest.mark.parametrize("link", ["file1.txt", "directory1"])
def test_registering_bad_relative_path(dummy_file, link):
    """
    Make sure we cannot register a datataset to a relative path that is already
    taken.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    data_path = str(tmp_src_dir / link)

    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_registering_bad_relative_path_{link}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
        relative_path=f"test/register/bad/relpath/{link}",
    )

    with pytest.raises(ValueError, match="combination is already"):
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_registering_bad_relative_path_2_{link}",
            "0.0.1",
            old_location=data_path,
            location_type="dataregistry",
            relative_path=f"test/register/bad/relpath/{link}",
        )


@pytest.mark.parametrize("link", ["file1.txt", "directory1"])
def test_registering_deleted_relative_path(dummy_file, link):
    """
    Should be able to use a relative_path of an old deleted dataset
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    data_path = str(tmp_src_dir / link)

    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_registering_deleted_relative_path_{link}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
        relative_path=f"my/relative/path/for/checking/{link}",
    )

    datareg.Registrar.dataset.delete(d_id)

    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_registering_deleted_relative_path_2_{link}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
        relative_path=f"my/relative/path/for/checking/{link}",
    )
