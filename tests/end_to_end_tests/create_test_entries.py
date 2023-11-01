import os
import sys

from dataregistry import DataRegistry

_TEST_ROOT_DIR = "DataRegistry_data"

# Make root dir
if not os.path.isdir(_TEST_ROOT_DIR):
    os.makedirs(_TEST_ROOT_DIR)

# Make a few dummy files to enter into database.
if not os.path.isdir(
    os.path.join(_TEST_ROOT_DIR, f"user/{os.getenv('USER')}/dummy_dir")
):
    os.makedirs(os.path.join(_TEST_ROOT_DIR, f"user/{os.getenv('USER')}/dummy_dir"))

if not os.path.isdir(os.path.join("dummy_dir")):
    os.makedirs(os.path.join("dummy_dir"))

with open(os.path.join("dummy_dir", "file1.txt"), "w") as f:
    f.write("test")
with open(
    os.path.join(_TEST_ROOT_DIR, f"user/{os.getenv('USER')}/dummy_dir", "file1.txt"),
    "w",
) as f:
    f.write("test")
with open(
    os.path.join(_TEST_ROOT_DIR, f"user/{os.getenv('USER')}/dummy_dir", "file2.txt"),
    "w",
) as f:
    f.write("test")
with open(
    os.path.join(_TEST_ROOT_DIR, f"user/{os.getenv('USER')}/", "file1.txt"), "w"
) as f:
    f.write("test")

# Establish connection to database
datareg = DataRegistry(root_dir=_TEST_ROOT_DIR)

def _insert_alias_entry(name, dataset_id):
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

    new_id = datareg.Registrar.register_dataset_alias(name, dataset_id)

    assert new_id is not None, "Trying to create a dataset alias that already exists"
    print(f"Created dataset alias entry with id {new_id}")

    return new_id


def _insert_execution_entry(
    name, description, input_datasets=[], configuration=None
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

    new_id = datareg.Registrar.register_execution(
        name,
        description=description,
        input_datasets=input_datasets,
        configuration=configuration,
    )

    assert new_id is not None, "Trying to create a execution that already exists"
    print(f"Created execution entry with id {new_id}")

    return new_id


def _insert_dataset_entry(
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
    new_id : int
        The dataset it created for this entry
    """

    if which_datareg is None:
        this_datareg = datareg
    else:
        this_datareg = which_datareg

    # Some defaults over all test datasets
    locale = "NERSC"
    creation_data = None
    make_sym_link = False

    # Add new entry.
    new_id = this_datareg.Registrar.register_dataset(
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
        input_datasets=input_datasets
    )

    assert new_id is not None, "Trying to create a dataset that already exists"
    print(f"Created dataset entry with id {new_id}")

    return new_id


# Test set 1
# - Auto create name for us
_insert_dataset_entry(
    "DESC/datasets/my_first_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first DESC dataset",
)

# Test set 2
# - Manual name
_insert_dataset_entry(
    "DESC/datasets/my_first_named_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first named DESC dataset",
    name="named_dataset",
)

# Test set 3
# - Test version bumping
_insert_dataset_entry(
    "DESC/datasets/bumped_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first bumped DESC dataset",
    name="bumped_dataset",
)

_insert_dataset_entry(
    "DESC/datasets/bumped_dataset_2",
    "patch",
    "user",
    None,
    "This is my second bumped DESC dataset",
    name="bumped_dataset",
)

_insert_dataset_entry(
    "DESC/datasets/bumped_dataset_3",
    "minor",
    "user",
    None,
    "This is my third bumped DESC dataset",
    name="bumped_dataset",
)

_insert_dataset_entry(
    "DESC/datasets/bumped_dataset_4",
    "major",
    "user",
    None,
    "This is my fourth bumped DESC dataset",
    name="bumped_dataset",
)

# Test set 4
# - Test user types
_insert_dataset_entry(
    "DESC/datasets/group1_dataset_1",
    "0.0.1",
    "group",
    "group1",
    "This is group 1's first dataset",
)

_insert_dataset_entry(
    "DESC/datasets/production_dataset_1",
    "0.0.1",
    "production",
    None,
    "This is production's first dataset",
)

# Test set 5
# - Create dataset aliases
dataset_id = _insert_dataset_entry(
    "DESC/datasets/production_dataset_with_horrible_name",
    "0.0.1",
    "production",
    None,
    "This is a production dataset",
)

_insert_alias_entry("nice_dataset_name", dataset_id)

# Test set 6
# - Create a pipeline with multiple input and output datasets.

# Stage 1 of my pipe line
ex_id_1 = _insert_execution_entry(
    "pipeline_stage_1", "The first stage of my pipeline"
)
dataset_id_1 = _insert_dataset_entry(
    "DESC/datasets/my_first_pipeline_stage1",
    "0.0.1",
    "user",
    None,
    "This is data for stage 1 of my first pipeline",
    execution_id=ex_id_1,
)

# Stage 2 of my pipeline
ex_id_2 = _insert_execution_entry(
    "pipeline_stage_2",
    "The second stage of my pipeline",
    input_datasets=[dataset_id_1],
)

dataset_id_2 = _insert_dataset_entry(
    "DESC/datasets/my_first_pipeline_stage2a",
    "0.0.1",
    "user",
    None,
    "This is data for stage 2 of my first pipeline",
    execution_id=ex_id_2,
)

dataset_id_3 = _insert_dataset_entry(
    "DESC/datasets/my_first_pipeline_stage2b",
    "0.0.1",
    "user",
    None,
    "This is data for stage 2 of my first pipeline",
    execution_id=ex_id_2,
)

# Stage 3 of my pipeline
ex_id_3 = _insert_execution_entry(
    "pipeline_stage_3",
    "The third stage of my pipeline",
    input_datasets=[dataset_id_2, dataset_id_3],
)

# Test set 7
# - Version suffixes
_insert_dataset_entry(
    "DESC/datasets/my_first_suffix_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first DESC dataset with a version suffix",
    name="my_first_suffix_dataset",
    version_suffix="test-suffix",
)

_insert_dataset_entry(
    "DESC/datasets/my_first_suffix_dataset_bumped",
    "minor",
    "user",
    None,
    "This is my first DESC dataset with a version suffix (bumped)",
    name="my_first_suffix_dataset",
    version_suffix="test-suffix",
)

# Test set 8
# - Include a configuration file in execution entry
_insert_execution_entry(
    "execution_with_configuration",
    "An execution with an input configuration file",
    configuration="dummy_configuration_file.yaml",
)

# Test set 9
# - Work with a real data
_insert_dataset_entry(
    "DESC/datasets/my_first_real_dataset_file",
    "0.0.1",
    "user",
    None,
    "This is my first DESC dataset with real files",
    is_dummy=False,
    old_location="dummy_configuration_file.yaml",
)

_insert_dataset_entry(
    "DESC/datasets/my_first_real_dataset_directory",
    "0.0.1",
    "user",
    None,
    "This is my second DESC dataset with real files",
    is_dummy=False,
    old_location="dummy_dir",
)

# Test set 10
# - Work with a real data already on location (i.e., old_location=None)
_insert_dataset_entry(
    "file1.txt",
    "0.0.1",
    "user",
    None,
    "This is my first DESC dataset with real files already on location",
    is_dummy=False,
    old_location=None,
    is_overwritable=True,
)

_insert_dataset_entry(
    "file1.txt",
    "0.0.2",
    "user",
    None,
    "This is my first DESC dataset with real files already on location (updated)",
    is_dummy=False,
    old_location=None,
)

_insert_dataset_entry(
    "dummy_dir",
    "0.0.1",
    "user",
    None,
    "This is my second DESC dataset with real files already on location",
    is_dummy=False,
    old_location=None,
    is_overwritable=True,
)

_insert_dataset_entry(
    "dummy_dir",
    "0.0.2",
    "user",
    None,
    "This is my second DESC dataset with real files already on location (updated)",
    is_dummy=False,
    old_location=None,
)

# Test set 11
# - Test global owner and owner types in the DataRegistry/Registar class
datareg2 = DataRegistry(root_dir=_TEST_ROOT_DIR, owner="DESC group", owner_type="group")

_insert_dataset_entry(
    "DESC/datasets/global_user_dataset",
    "0.0.1",
    None,
    None,
    "This should be owned by 'DESC group' and have owner_type='group'",
    which_datareg=datareg2
)

# Test set 12
# - Testing execution creation directly through dataset registration
_insert_dataset_entry(
    "DESC/datasets/execution_test",
    "0.0.1",
    None,
    None,
    "This should have a more descriptive execution",
    execution_name="Overwrite execution auto name",
    execution_description="Overwrite execution auto description",
    execution_locale="TestMachine",
    input_datasets=[dataset_id_1],
)
