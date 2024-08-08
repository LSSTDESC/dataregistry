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


@pytest.mark.parametrize(
    "_REL_PATH,name_tag",
    [(None, ""), ("test_replace_dataset/relpath", "_with_relpath")],
)
def test_replace_dataset(dummy_file, _REL_PATH, name_tag):
    """Test registering a dataset, then replacing it"""

    _NAME = f"DESC:dataset:test_replace_dataset{name_tag}"

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add a dataset
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
        is_overwritable=True,
        description="Dataset before replace",
        relative_path=_REL_PATH,
    )

    # Add another dataset to replace the first
    d2_id = _replace_dataset_entry(
        datareg,
        _NAME,
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
            "dataset.relative_path",
        ],
        [f],
    )

    assert len(results["dataset.name"]) == 1

    assert results["dataset.name"][0] == _NAME
    assert results["dataset.version_string"][0] == "0.0.1"
    assert results["dataset.description"][0] == "Dataset before replace"
    assert results["dataset.replace_date"][0] is not None
    assert results["dataset.replace_uid"][0] is not None
    assert results["dataset.replace_id"][0] == d2_id

    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d2_id)
    results2 = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.description",
            "dataset.replace_date",
            "dataset.replace_uid",
            "dataset.replace_id",
            "dataset.relative_path",
        ],
        [f],
    )

    assert len(results2["dataset.name"]) == 1

    assert results2["dataset.name"][0] == _NAME
    assert results2["dataset.version_string"][0] == "0.0.1"
    assert results2["dataset.description"][0] == "Dataset after replace"
    assert results2["dataset.replace_date"][0] is None
    assert results2["dataset.replace_uid"][0] is None
    assert results2["dataset.replace_id"][0] is None

    # Make sure the relative paths are the same for each dataset
    assert results["dataset.relative_path"][0] == results2["dataset.relative_path"][0]
