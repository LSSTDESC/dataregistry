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
    "data_org,data_path,v_str,overwritable",
    [
        ("file", "file1.txt", "0.0.1", True),
        ("file", "file1.txt", "0.0.2", True),
        ("file", "file1.txt", "0.0.3", False),
        ("directory", "dummy_dir", "0.0.1", True),
        ("directory", "dummy_dir", "0.0.2", True),
        ("directory", "dummy_dir", "0.0.3", False),
    ],
)
def test_on_location_data(dummy_file, data_org, data_path, v_str, overwritable):
    """
    Test ingesting real data into the registry (already on location). Also
    tests overwriting datasets.

    Does three times for each file, the first is a normal entry with
    `is_overwritable=True`. The second and third tests overwriting the previous
    data with a new version.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:test_on_location_data_{data_org}",
        v_str,
        old_location=None,
        location_type="dataregistry",
        is_overwritable=overwritable,
        relative_path=data_path,
    )

    f = datareg.Query.gen_filter("dataset.relative_path", "==", data_path)
    results = datareg.Query.find_datasets(
        [
            "dataset.data_org",
            "dataset.nfiles",
            "dataset.total_disk_space",
            "dataset.is_overwritable",
            "dataset.is_overwritten",
            "dataset.version_string",
        ],
        [f],
        return_format="cursorresult",
    )

    num_results = len(results.all())
    for i, r in enumerate(results):
        assert getattr(r, "dataset.data_org") == data_org
        assert getattr(r, "dataset.nfiles") == 1
        assert getattr(r, "dataset.total_disk_space") > 0
        if getattr(r, "version_string") == "0.0.1":
            if num_results == 1:
                assert getattr(r, "dataset.is_overwritable") == True
                assert getattr(r, "dataset.is_overwritten") == False
            else:
                assert getattr(r, "dataset.is_overwritable") == True
                assert getattr(r, "dataset.is_overwritten") == True
        elif getattr(r, "version_string") == "0.0.2":
            assert num_results >= 2
            if num_results == 2:
                assert getattr(r, "dataset.is_overwritable") == True
                assert getattr(r, "dataset.is_overwritten") == False
            elif num_results == 3:
                assert getattr(r, "dataset.is_overwritable") == True
                assert getattr(r, "dataset.is_overwritten") == True
        elif getattr(r, "version_string") == "0.0.3":
            assert num_results >= 3
            if num_results == 3:
                assert getattr(r, "dataset.is_overwritable") == False
                assert getattr(r, "dataset.is_overwritten") == False
            else:
                assert getattr(r, "dataset.is_overwritable") == True
                assert getattr(r, "dataset.is_overwritten") == True


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
        relative_path=f"my/relative/path/for/checking/{link}",
    )

    with pytest.raises(ValueError, match="combination is already"):
        d_id = _insert_dataset_entry(
            datareg,
            f"DESC:datasets:test_registering_bad_relative_path_2_{link}",
            "0.0.1",
            old_location=data_path,
            location_type="dataregistry",
            relative_path=f"my/relative/path/for/checking/{link}",
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
