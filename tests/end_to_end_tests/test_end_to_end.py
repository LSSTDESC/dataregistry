import os
import sys
import yaml

from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
from dataregistry.registrar import _OWNER_TYPES
import pytest

from dataregistry.registrar.registrar_util import _form_dataset_path


@pytest.fixture
def dummy_file(tmp_path):
    """
    Create some dummy (temporary) files and directories

    Parameters
    ----------
    tmp_path : pathlib.Path object

    Returns
    -------
    tmp_src_dir : pathlib.Path object
        Temporary files we are going to be copying into the registry will be
        created in here
    tmp_root_dir : pathlib.Path object
        Temporary root_dir for the registry we can copy files to
    """

    # Temp dir for files that we copy files from (old_location)
    tmp_src_dir = tmp_path / "source"
    tmp_src_dir.mkdir()

    for i in range(2):
        f = tmp_src_dir / f"file{i+1}.txt"
        f.write_text("i am a dummy file")

    p = tmp_src_dir / "directory1"
    p.mkdir()
    f = p / "file2.txt"
    f.write_text("i am another dummy file")

    # Temp root_dir of the registry
    tmp_root_dir = tmp_path / "root_dir"
    for THIS_SCHEMA in [SCHEMA_VERSION + "/", ""]:
        p = tmp_root_dir / f"{THIS_SCHEMA}user/{os.getenv('USER')}/dummy_dir"
        p.mkdir(parents=True)

        f = p / "file1.txt"
        f.write_text("i am another dummy file (but on location in a dir)")

        p = tmp_root_dir / f"{THIS_SCHEMA}user/{os.getenv('USER')}"
        f = p / "file1.txt"
        f.write_text("i am another dummy file (but on location)")

    # Make a dummy configuration yaml file
    data = {
        "run_by": "somebody",
        "software_version": {"major": 1, "minor": 1, "patch": 0},
        "an_important_list": [1, 2, 3],
    }

    # Write the data to the YAML file
    with open(tmp_src_dir / "dummy_configuration_file.yaml", "w") as file:
        yaml.dump(data, file, default_flow_style=False)

    return tmp_src_dir, tmp_root_dir


def _insert_alias_entry(datareg, name, dataset_id):
    """
    Wrapper to create dataset alias entry

    Parameters
    ----------
    name : str
        Name of alias
    dataset_id : int
        Dataset we are assigning alias name to

    Returns
    -------
    new_id : int
        The alias ID for this new entry
    """

    new_id = datareg.Registrar.dataset_alias.create(name, dataset_id)

    assert new_id is not None, "Trying to create a dataset alias that already exists"
    print(f"Created dataset alias entry with id {new_id}")

    return new_id


def _insert_execution_entry(
    datareg, name, description, input_datasets=[], configuration=None
):
    """
    Wrapper to create execution entry

    Parameters
    ----------
    name : str
        Name of execution
    description : str
        Description of execution
    intput_datasets : list
        List of dataset ids
    configuration : str
        Path to configuration file for execution

    Returns
    -------
    new_id : int
        The execution ID for this new entry
    """

    new_id = datareg.Registrar.execution.create(
        name,
        description=description,
        input_datasets=input_datasets,
        configuration=configuration,
    )

    assert new_id is not None, "Trying to create a execution that already exists"
    print(f"Created execution entry with id {new_id}")

    return new_id


def _insert_dataset_entry(
    datareg,
    relpath,
    version,
    owner_type,
    owner,
    description,
    name=None,
    execution_id=None,
    version_suffix=None,
    is_dummy=True,
    old_location=None,
    is_overwritable=False,
    which_datareg=None,
    execution_name=None,
    execution_description=None,
    execution_start=None,
    execution_locale=None,
    execution_configuration=None,
    input_datasets=[],
):
    """
    Wrapper to create dataset entry

    Parameters
    ----------
    relpath : str
        Relative path within the data registry to store the data
        Relative to <ROOT>/<owner_type>/<owner>/...
    version : str
        Semantic version string (i.e., M.N.P) or
        "major", "minor", "patch" to automatically bump the version previous
    owner_type : str
        Either "production", "group", "user"
    owner : str
        Dataset owner
    description : str
        Description of dataset
    name : str
        A manually selected name for the dataset
    execution_id : int
        Execution entry related to this dataset
    version_suffix : str
        Append a suffix to the version string
    is_dummy : bool
        True for dummy dataset (copies no data)
    old_location : str
        Path to data to be copied to data registry
    which_datareg : DataRegistry object
        In case we want to register using a custom DataRegistry object
    execution_name : str, optional
            Typically pipeline name or program name
    execution_description : str, optional
        Human readible description of execution
    execution_start : datetime, optional
        Date the execution started
    execution_locale : str, optional
        Where was the execution performed?
    execution_configuration : str, optional
        Path to text file used to configure the execution
    input_datasets : list, optional
        List of dataset ids that were the input to this execution

    Returns
    -------
    dataset_id : int
        The dataset it created for this entry
    """

    # Some defaults over all test datasets
    locale = "NERSC"
    creation_data = None
    make_sym_link = False

    # Add new entry.
    dataset_id, execution_id = datareg.Registrar.dataset.create(
        relpath,
        version,
        version_suffix=version_suffix,
        name=name,
        creation_date=creation_data,
        description=description,
        old_location=old_location,
        copy=(not make_sym_link),
        is_dummy=is_dummy,
        execution_id=execution_id,
        verbose=True,
        owner=owner,
        owner_type=owner_type,
        is_overwritable=is_overwritable,
        execution_name=execution_name,
        execution_description=execution_description,
        execution_start=execution_start,
        execution_locale=execution_locale,
        execution_configuration=execution_configuration,
        input_datasets=input_datasets,
    )

    assert dataset_id is not None, "Trying to create a dataset that already exists"
    assert execution_id is not None, "Trying to create a execution that already exists"
    print(f"Created dataset entry with id {dataset_id}")

    return dataset_id


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
        "user",
        None,
        "This is my first DESC dataset",
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
        "user",
        None,
        "This is my first DESC dataset",
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


@pytest.mark.parametrize(
    "v_type,ans,name",
    [
        ("major", "1.0.0", "my_first_dataset"),
        ("minor", "0.1.0", "my_first_dataset"),
        ("patch", "0.0.2", "my_first_dataset"),
        ("patch", "0.0.1", "my_second_dataset"),
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
        f"DESC/datasets/bumped_dataset_{v_type}_{name}",
        v_type,
        "user",
        None,
        "This is my first bumped DESC dataset",
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
        owner_type,
        None,
        f"This is a {owner_type} dataset",
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
        "user",
        None,
        "Test copying a real file",
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
        ("file", "file1.txt", "0.0.2", False),
        ("directory", "dummy_dir", "0.0.1", True),
        ("directory", "dummy_dir", "0.0.2", False),
    ],
)
def test_on_location_data(dummy_file, data_org, data_path, v_str, overwritable):
    """
    Test ingesting real data into the registry (already on location). Also
    tests overwriting datasets.

    Does twice for each file, the first is a normal entry with
    `is_overwritable=True`. The second tests overwriting the previous data with
    a new version.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    d_id = _insert_dataset_entry(
        datareg,
        data_path,
        v_str,
        "user",
        None,
        "Test ingesting a real file on location",
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
        else:
            if num_results == 1:
                assert getattr(r, "dataset.is_overwritable") == False
                assert getattr(r, "dataset.is_overwritten") == True
            else:
                assert getattr(r, "dataset.is_overwritable") == False
                assert getattr(r, "dataset.is_overwritten") == False
        assert i < 2


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
        "user",
        None,
        "Test dataset alias",
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
        "user",
        None,
        "This is data for stage 1 of my first pipeline",
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
        "user",
        None,
        "This is data for stage 2 of my first pipeline",
        execution_id=ex_id_2,
    )

    d_id_3 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_first_pipeline_stage2b",
        "0.0.1",
        "user",
        None,
        "This is data for stage 2 of my first pipeline",
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
        None,
        None,
        "Should be allocated user and user_type from global config",
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
        "production",
        None,
        "This is production's first dataset",
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
        None,
        None,
        "This is production's first dataset",
    )

    d_id_2 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/execution_test",
        "0.0.1",
        None,
        None,
        "This should have a more descriptive execution",
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
        dset_ownertype,
        dset_owner,
        "Test the Query.get_dataset_absolute_path function",
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


def test_delete_entry_dummy(dummy_file):
    """Make a simple (dummy) entry, then delete it, then check it was deleted"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Make sure we raise an exception trying to delete a dataset that doesn't exist
    with pytest.raises(ValueError, match="does not exist"):
        datareg.Registrar.dataset.delete(10000)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/dummy_dataset_to_delete",
        "0.0.1",
        "user",
        None,
        "A dataset to delete",
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
        ],
        [f],
        return_format="cursorresult",
    )

    for r in results:
        assert getattr(r, "dataset.status") == 3
        assert getattr(r, "dataset.delete_date") is not None
        assert getattr(r, "dataset.delete_uid") is not None


def test_delete_entry_real(dummy_file):
    """Make a simple (real data) entry, then delete it, then check it was deleted"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Make sure we raise an exception trying to delete a dataset that doesn't exist
    with pytest.raises(ValueError, match="does not exist"):
        datareg.Registrar.dataset.delete(10000)

    # Add entry
    data_path = str(tmp_src_dir / "file2.txt")
    assert os.path.isfile(data_path)
    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/real_dataset_to_delete",
        "0.0.1",
        "user",
        None,
        "A dataset to delete",
        old_location=data_path,
        is_dummy=False,
    )

    # Now delete that entry
    datareg.Registrar.dataset.delete(d_id)

    # Check the entry was set to deleted in the registry
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.status",
            "dataset.delete_date",
            "dataset.delete_uid",
            "dataset.owner",
            "dataset.owner_type",
            "dataset.relative_path",
        ],
        [f],
        return_format="cursorresult",
    )

    for r in results:
        assert getattr(r, "dataset.status") == 3
        assert getattr(r, "dataset.delete_date") is not None
        assert getattr(r, "dataset.delete_uid") is not None

    # Make sure the file in the root_dir has gone
    data_path = _form_dataset_path(
        getattr(r, "dataset.owner_type"),
        getattr(r, "dataset.owner"),
        getattr(r, "dataset.relative_path"),
        schema=SCHEMA_VERSION,
        root_dir=str(tmp_root_dir),
    )
    assert not os.path.isfile(data_path)
