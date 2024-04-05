import os
import sys
import yaml

from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
import pytest

from dataregistry.registrar.registrar_util import _form_dataset_path
from dataregistry.registrar.dataset_util import set_dataset_status, get_dataset_status
from database_test_utils import *

def test_simple_query(dummy_file):
    """Make a simple entry, and make sure the query returns the correct result"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_first_dataset",
        "0.0.1",
        description="This is my first DESC dataset",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.owner",
            "dataset.owner_type",
            "dataset.description",
            "dataset.version_major",
            "dataset.version_minor",
            "dataset.version_patch",
            "dataset.relative_path",
            "dataset.version_suffix",
            "dataset.data_org",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == "my_first_dataset"
        assert getattr(r, "dataset.version_string") == "0.0.1"
        assert getattr(r, "dataset.version_major") == 0
        assert getattr(r, "dataset.version_minor") == 0
        assert getattr(r, "dataset.version_patch") == 1
        assert getattr(r, "dataset.owner") == os.getenv("USER")
        assert getattr(r, "dataset.owner_type") == "user"
        assert getattr(r, "dataset.description") == "This is my first DESC dataset"
        assert getattr(r, "dataset.relative_path") == "DESC/datasets/my_first_dataset"
        assert getattr(r, "dataset.version_suffix") == None
        assert getattr(r, "dataset.data_org") == "dummy"
        assert i < 1


def test_manual_name_and_vsuffix(dummy_file):
    """Test setting the name and version suffix manually"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_second_dataset",
        "0.0.1",
        name="custom name",
        version_suffix="custom_suffix",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_suffix"], [f], return_format="cursorresult"
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == "custom name"
        assert getattr(r, "dataset.version_suffix") == "custom_suffix"
        assert i < 1

    # Try to bump dataset with version suffix (should fail)
    with pytest.raises(ValueError, match="Cannot bump"):
        d_id = _insert_dataset_entry(
            datareg,
            "DESC/datasets/my_second_dataset_bumped",
            "major",
            name="custom name",
        )


@pytest.mark.parametrize(
    "v_type,ans,name",
    [
        ("major", "1.0.0", "my_first_dataset"),
        ("minor", "1.1.0", "my_first_dataset"),
        ("patch", "1.1.1", "my_first_dataset"),
        ("patch", "1.1.2", "my_first_dataset"),
        ("minor", "1.2.0", "my_first_dataset"),
        ("major", "2.0.0", "my_first_dataset"),
    ],
)
def test_dataset_bumping(dummy_file, v_type, ans, name):
    """
    Test bumping a dataset and make sure the new version is correct.

    Tests bumping datasets with and without a version suffix.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC/datasets/bumped_dataset_{v_type}_{name}_{ans.replace('.','_')}",
        v_type,
        name=name,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string"], [f], return_format="cursorresult"
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.name") == name
        assert getattr(r, "dataset.version_string") == ans
        assert i < 1


@pytest.mark.parametrize("owner_type", ["user", "group", "project"])
def test_owner_types(dummy_file, owner_type):
    """Test the different owner types"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC/datasets/owner_type_{owner_type}",
        "0.0.1",
        owner_type=owner_type,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.owner_type"], [f], return_format="cursorresult"
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.owner_type") == owner_type
        assert i < 1


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
        f"DESC/datasets/copy_real_{data_org}",
        "0.0.1",
        old_location=data_path,
        is_dummy=False,
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
        data_path,
        v_str,
        old_location=None,
        is_dummy=False,
        is_overwritable=overwritable,
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


def test_dataset_alias(dummy_file):
    """Register a dataset and make a dataset alias entry for it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add dataset
    d_id = _insert_dataset_entry(
        datareg,
        "alias_test_entry",
        "0.0.1",
    )

    # Add alias
    _insert_alias_entry(datareg, "nice_dataset_name", d_id)

    # Query
    f = datareg.Query.gen_filter("dataset_alias.alias", "==", "nice_dataset_name")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset_alias.dataset_id",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.dataset_id") == d_id
        assert getattr(r, "dataset_alias.dataset_id") == d_id
        assert i < 1


def test_pipeline_entry(dummy_file):
    """
    Test making multiple executions and datasets to form a pipeline.

    Also queries to make sure dependencies are made.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entries
    ex_id_1 = _insert_execution_entry(
        datareg, "pipeline_stage_1", "The first stage of my pipeline"
    )

    d_id_1 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_first_pipeline_stage1",
        "0.0.1",
        execution_id=ex_id_1,
    )

    ex_id_2 = _insert_execution_entry(
        datareg,
        "pipeline_stage_2",
        "The second stage of my pipeline",
        input_datasets=[d_id_1],
    )

    d_id_2 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_first_pipeline_stage2a",
        "0.0.1",
        execution_id=ex_id_2,
    )

    d_id_3 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_first_pipeline_stage2b",
        "0.0.1",
        execution_id=ex_id_2,
    )

    # Stage 3 of my pipeline
    ex_id_3 = _insert_execution_entry(
        datareg,
        "pipeline_stage_3",
        "The third stage of my pipeline",
        input_datasets=[d_id_2, d_id_3],
    )

    # Query on execution
    f = datareg.Query.gen_filter("dataset.execution_id", "==", ex_id_2)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert "my_first_pipeline_stage2" in getattr(r, "dataset.name")
        assert i < 2

    # Query on dependency
    f = datareg.Query.gen_filter("dependency.execution_id", "==", ex_id_2)
    results = datareg.Query.find_datasets(
        [
            "dependency.execution_id",
            "dataset.dataset_id",
            "dataset.execution_id",
            "dataset.name",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.dataset_id") == d_id_1
        assert i < 1


def test_global_owner_set(dummy_file):
    """
    Test setting the owner and owner_type globally during the database
    initialization.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        schema=SCHEMA_VERSION,
        owner="DESC group",
        owner_type="group",
    )

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/global_user_dataset",
        "0.0.1",
        owner=None,
        owner_type=None,
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.owner",
            "dataset.owner_type",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.owner") == "DESC group"
        assert getattr(r, "dataset.owner_type") == "group"
        assert i < 1


@pytest.mark.skip(reason="Can't do production related things with sqlite")
def test_prooduction_schema(dummy_file):
    """
    Test making multiple executions and datasets to form a pipeline.

    Also queries to make sure dependencies are made.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema="production")

    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/production_dataset_1",
        "0.0.1",
        owner_type="production",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.owner",
            "dataset.owner_type",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.owner") == "production"
        assert getattr(r, "dataset.owner_type") == "production"
        assert i < 1


def test_execution_config_file(dummy_file):
    """Test ingesting a configuration file with an execution entry"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add entry
    ex_id = _insert_execution_entry(
        datareg,
        "execution_with_configuration",
        "An execution with an input configuration file",
        configuration=str(tmp_src_dir / "dummy_configuration_file.yaml"),
    )

    # Query
    f = datareg.Query.gen_filter("execution.execution_id", "==", ex_id)
    results = datareg.Query.find_datasets(
        [
            "execution.configuration",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "execution.configuration") is not None
        assert i < 1


def test_dataset_with_execution(dummy_file):
    """
    Test modifying the datasets default execution directly when registering the
    dataset
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    d_id_1 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/execution_test_input",
        "0.0.1",
    )

    d_id_2 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/execution_test",
        "0.0.1",
        execution_name="Overwrite execution auto name",
        execution_description="Overwrite execution auto description",
        execution_locale="TestMachine",
        input_datasets=[d_id_1],
    )

    # Query on execution
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id_2)
    results = datareg.Query.find_datasets(
        [
            "dataset.name",
            "execution.execution_id",
            "execution.description",
            "execution.locale",
            "execution.name",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "execution.name") == "Overwrite execution auto name"
        assert (
            getattr(r, "execution.description")
            == "Overwrite execution auto description"
        )
        assert getattr(r, "execution.locale") == "TestMachine"
        ex_id_1 = getattr(r, "execution.execution_id")
        assert i < 1

    # Query on dependency
    f = datareg.Query.gen_filter("dependency.input_id", "==", d_id_1)
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dependency.execution_id",
            "dependency.input_id",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dependency.execution_id") == ex_id_1
        assert i < 1


def test_get_dataset_absolute_path(dummy_file):
    """
    Test the generation of the full absolute path of a dataset using the
    `Query.get_dataset_absolute_path` function
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    dset_relpath = "DESC/datasets/get_dataset_absolute_path_test"
    dset_ownertype = "group"
    dset_owner = "group1"

    # Make a basic entry
    d_id_1 = _insert_dataset_entry(
        datareg,
        dset_relpath,
        "0.0.1",
        owner_type=dset_ownertype,
        owner=dset_owner,
    )

    v = datareg.Query.get_dataset_absolute_path(d_id_1)

    if datareg.Query._dialect == "sqlite":
        assert v == os.path.join(
            str(tmp_root_dir), dset_ownertype, dset_owner, dset_relpath
        )
    else:
        assert v == os.path.join(
            str(tmp_root_dir), SCHEMA_VERSION, dset_ownertype, dset_owner, dset_relpath
        )


@pytest.mark.parametrize(
    "is_dummy,dataset_name",
    [
        (True, "dummy_dataset_to_delete"),
        (False, "real_dataset_to_delete"),
        (False, "real_directory_to_delete"),
    ],
)
def test_delete_entry(dummy_file, is_dummy, dataset_name):
    """
    Make a simple entry, then delete it, then check it was deleted.

    Does this for a dummy dataset and a real one.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Make sure we raise an exception trying to delete a dataset that doesn't exist
    with pytest.raises(ValueError, match="not found in"):
        datareg.Registrar.dataset.delete(10000)

    # Where is the real data?
    if is_dummy:
        data_path = None
    else:
        if dataset_name == "real_dataset_to_delete":
            data_path = str(tmp_src_dir / "file2.txt")
            assert os.path.isfile(data_path)
        else:
            data_path = str(tmp_src_dir / "directory1")
            assert os.path.isdir(data_path)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC/datasets/{dataset_name}",
        "0.0.1",
        is_dummy=is_dummy,
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
