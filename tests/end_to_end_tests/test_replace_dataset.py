from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

from database_test_utils import *
import pytest


def test_register_dataset_twice(dummy_file):
    """Test registering a dataset twice, should fail"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add two dataset
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:dataset:test_register_dataset_twice",
        "0.0.1",
    )

    with pytest.raises(ValueError, match="There is already a dataset"):
        d2_id = _insert_dataset_entry(
            datareg,
            "DESC:dataset:test_register_dataset_twice",
            "0.0.1",
        )


def test_replace_dataset(dummy_file):
    """Test registering a dataset, then replacing it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add a dataset
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:dataset:test_replace_dataset",
        "0.0.1",
        is_overwritable=True,
        description="Dataset before replace",
    )

    # Add another dataset to replace the first
    d2_id = _replace_dataset_entry(
        datareg,
        "DESC:dataset:test_replace_dataset",
        "0.0.1",
        description="Dataset after replace",
    )

    # Query and make sure it was replaced properly
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.description",
            "dataset.replace_date",
            "dataset.replace_uid",
            "dataset.replace_id",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert i < 1
        assert getattr(r, "dataset.name") == "DESC:dataset:test_replace_dataset"
        assert getattr(r, "dataset.version_string") == "0.0.1"
        assert getattr(r, "dataset.description") == "Dataset before replace"
        assert getattr(r, "dataset.replace_date") is not None
        assert getattr(r, "dataset.replace_uid") is not None
        assert getattr(r, "dataset.replace_id") == d2_id

    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d2_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.description",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert i < 1
        assert getattr(r, "dataset.name") == "DESC:dataset:test_replace_dataset"
        assert getattr(r, "dataset.version_string") == "0.0.1"
        assert getattr(r, "dataset.description") == "Dataset after replace"
