import os
import sys

import pytest
import yaml
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_SCHEMA_WORKING
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path
from dataregistry.exceptions import DataRegistryRootDirBadState
from database_test_utils import *


@pytest.mark.parametrize("data_org", ["file", "directory"])
def test_copy_data(dummy_file, data_org):
    """Test copying real data into the registry (from an `old_location`)"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

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
        ("same_directory", "dummy_dir")
    ],
)
def test_on_location_data(dummy_file, data_org, data_path):
    """
    Test ingesting real data into the registry (already on location).
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

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

    if data_org == "same_directory":
        assert len(results["dataset.data_org"]) == 2
    else:
        assert len(results["dataset.data_org"]) == 1

    assert data_org.endswith(results["dataset.data_org"][0])
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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    data_path = str(tmp_src_dir / link)

    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_registering_bad_relative_path_{link}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
        relative_path=f"test/register/bad/relpath/{link}",
    )

    with pytest.raises(ValueError, match="is taken by"):
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

    Replace the original dataset first to make sure all the internal checks
    with `replace_iteration` work.
    """

    _N_REPLACE = 6

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    data_path = str(tmp_src_dir / link)

    # Original insert
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_registering_deleted_relative_path_{link}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
        relative_path=f"my/relative/path/for/checking/{link}",
        is_overwritable=True,
    )

    # Replace original a few times
    for i in range(_N_REPLACE):
        d2_id = _replace_dataset_entry(
            datareg,
            f"DESC:datasets:test_registering_deleted_relative_path_{link}",
            "0.0.1",
            old_location=data_path,
            is_overwritable=True,
        )

    # Now delete
    datareg.Registrar.dataset.delete(d2_id)

    # Make a new entry (new name) using the original relative path
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_registering_deleted_relative_path_2_{link}",
        "0.0.1",
        old_location=data_path,
        location_type="dataregistry",
        relative_path=f"my/relative/path/for/checking/{link}",
        is_overwritable=True,
    )

    # Replace a few times (less times than the original, so `replace_iteration`
    # is lower)
    for i in range(_N_REPLACE // 2):
        d2_id = _replace_dataset_entry(
            datareg,
            f"DESC:datasets:test_registering_deleted_relative_path_2_{link}",
            "0.0.1",
            old_location=data_path,
            is_overwritable=True,
        )

    # Now register third and final time, new name, same relative path, should
    # fail as path is still taken by the entry above
    with pytest.raises(ValueError, match="is taken by"):
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_registering_deleted_relative_path_3_{link}",
            "0.0.1",
            old_location=data_path,
            location_type="dataregistry",
            relative_path=f"my/relative/path/for/checking/{link}",
        )


@pytest.mark.parametrize(
    "link,dest", [["file1.txt", "file2.txt"], ["directory1", "dummy_dir_2"]]
)
def test_registering_data_already_there(dummy_file, link, dest):
    """
    When ingesting data into the `root_dir` with `old_location`, no data should
    exist at that `relative_path`.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    data_path = str(tmp_src_dir / link)

    with pytest.raises(DataRegistryRootDirBadState, match="data already exists at"):
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_registering_data_already_there_{link}",
            "0.0.1",
            old_location=data_path,
            relative_path=dest,
            location_type="dataregistry",
        )
