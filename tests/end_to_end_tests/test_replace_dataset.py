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
            relative_path="test_register_dataset_twice/test"
        )


def _check_replaced_dataset(
    datareg,
    d_id,
    name,
    version,
    description,
    replace_id,
    replace_iteration,
    is_deleted,
    relative_path=None,
):
    """
    Helper function to make sure a dataset has been replaced properly

    Parameters
    ----------
    datareg : DataRegistry object
    d_id : int
        The dataset ID we are checking
    name / version / description : str
        Dataset name / version_string / descriptions
    replace_id : int
        The ID of dataset that replaced this dataset (if replaced)
    replace_iteration : int
        What replace iteration should this dataset be at
    is_deleted : bool
        True if we expect this dataset to have been deleted
    relative_path : str

    Returns
    -------
    relative_path : str
    """

    # Check replaced dataset
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.description",
            "dataset.delete_date",
            "dataset.delete_uid",
            "dataset.replace_id",
            "dataset.relative_path",
            "dataset.replace_iteration",
        ],
        [f],
    )

    assert len(results["dataset.name"]) == 1

    assert results["dataset.name"][0] == name
    assert results["dataset.version_string"][0] == version
    if description is None:
        assert results["dataset.description"][0] is None
    else:
        assert results["dataset.description"][0] == description
    if replace_id is None:
        assert results["dataset.replace_id"][0] is None
    else:
        assert results["dataset.replace_id"][0] is not None
    if is_deleted:
        assert results["dataset.delete_date"][0] is not None
        assert results["dataset.delete_uid"][0] is not None
    else:
        assert results["dataset.delete_date"][0] is None
        assert results["dataset.delete_uid"][0] is None
    assert results["dataset.replace_iteration"][0] == replace_iteration

    if relative_path is not None:
        assert results["dataset.relative_path"][0] == relative_path

    return results["dataset.relative_path"][0]


@pytest.mark.parametrize(
    "_REL_PATH,name_tag",
    [(None, ""), ("test_replace_dataset/relpath", "_with_relpath")],
)
def test_replace_dataset(dummy_file, _REL_PATH, name_tag):
    """
    Test registering a dataset, then replacing it using the `replace` function.

    Test with and without a manual `relative_path`. The replaced dataset should
    keep the relative path of the original, as well as the
    name/version/suffix/owner/owner_type combination. All other properties are
    not carried over to the new dataset.
    """

    _NAME = f"DESC:dataset:test_replace_dataset{name_tag}"
    _N_REPLACE = 3  # Replace this many times

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

    # Replace the dataset N times
    for i in range(_N_REPLACE):
        # Add another dataset to replace the first
        d2_id = _replace_dataset_entry(
            datareg,
            _NAME,
            "0.0.1",
            is_overwritable=True,
            description="Dataset after replace",
        )
        if i == 0:
            first_replace_id = d2_id

        # Check replaced dataset
        rel_path = _check_replaced_dataset(
            datareg, d2_id, _NAME, "0.0.1", "Dataset after replace", None, i + 1, False
        )

    # Check first dataset
    _check_replaced_dataset(
        datareg,
        d_id,
        _NAME,
        "0.0.1",
        "Dataset before replace",
        first_replace_id,
        0,
        True,
        relative_path=rel_path,
    )


def test_replacing_deleted_dataset(dummy_file):
    """Should not be able to replace a dataset that has been previously deleted"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    _NAME = "DESC:dataset:test_replacing_deleted_dataset"

    # Add dataset
    d_id = _insert_dataset_entry(
        datareg,
        _NAME,
        "0.0.1",
        is_overwritable=True,
    )

    # Delete dataset
    datareg.Registrar.dataset.delete(d_id)

    with pytest.raises(ValueError, match="is deleted, cannot replace"):
        # Try to replace deleted dataset
        d2_id = _replace_dataset_entry(
            datareg,
            _NAME,
            "0.0.1",
        )

def test_replacing_non_overwritable_dataset(dummy_file):
    """Should not be able to replace a non-overwritable dataset"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add dataset
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:dataset:test_replacing_non_overwritable_dataset",
        "0.0.1",
        is_overwritable=False,
    )

    # Try to replace non-overwritable dataset (should fail)
    with pytest.raises(ValueError, match="is not overwritable"):
        d2_id = _replace_dataset_entry(
            datareg,
            "DESC:dataset:test_replacing_non_overwritable_dataset",
            "0.0.1",
        )
