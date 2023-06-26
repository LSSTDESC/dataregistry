import os
import sys

from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine, ownertypeenum
from dataregistry.registrar import Registrar

_lookup = {
    "production": ownertypeenum.production,
    "group": ownertypeenum.group,
    "user": ownertypeenum.user,
}

# Locate dataregistry configuration file.
if os.getenv("DREGS_CONFIG") is None:
    raise Exception("Need to set DREGS_CONFIG env variable")
else:
    DREGS_CONFIG = os.getenv("DREGS_CONFIG")

# Establish connection to database
engine, dialect = create_db_engine(config_file=DREGS_CONFIG)


def _insert_alias_entry(name, dataset_id, owner_type, owner):
    """
    Wrapper to create dataset alias entry

    Parameters
    ----------
    name : str
        Name of alias
    dataset_id : int
        Dataset we are assigning alias name to
    owner_type : str
        Either "production", "group", "user"
    owner : str
        Dataset owner

    Returns
    -------
    new_id : int
        The alias ID for this new entry
    """

    # Create Registrar object
    registrar = Registrar(
        engine, dialect, _lookup[owner_type], owner=owner, schema_version=SCHEMA_VERSION
    )

    new_id = registrar.register_dataset_alias(name, dataset_id,)

    assert new_id is not None, "Trying to create a dataset alias that already exists"
    print(f"Created dataset alias entry with id {new_id}")

    return new_id


def _insert_execution_entry(
    name, description, owner_type, owner, input_datasets=[], configuration=None
):
    """
    Wrapper to create execution entry

    Parameters
    ----------
    name : str
        Name of execution
    description : str
        Description of execution
    owner_type : str
        Either "production", "group", "user"
    owner : str
        Dataset owner
    intput_datasets : list
        List of dataset ids
    contiguration : str
        Path to configuration file for execution

    Returns
    -------
    new_id : int
        The execution ID for this new entry
    """

    # Create Registrar object
    registrar = Registrar(
        engine, dialect, _lookup[owner_type], owner=owner, schema_version=SCHEMA_VERSION
    )

    new_id = registrar.register_execution(
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

    Returns
    -------
    new_id : int
        The dataset it created for this entry
    """

    # Some defaults over all test datasets
    locale = "NERSC"
    is_overwritable = False
    creation_data = None
    make_sym_link = False
    if owner is None:
        owner = os.getenv("USER")

    # Create Registrar object
    registrar = Registrar(
        engine, dialect, _lookup[owner_type], owner=owner, schema_version=SCHEMA_VERSION
    )

    # Add new entry.
    new_id = registrar.register_dataset(
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

_insert_alias_entry("nice_dataset_name", dataset_id, "production", None)

# Test set 6
# - Create a pipeline with multiple input and output datasets.

# Stage 1 of my pipe line
ex_id_1 = _insert_execution_entry(
    "pipeline_stage_1", "The first stage of my pipeline", "user", None
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
    "user",
    None,
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
    "user",
    None,
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
    "user",
    None,
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
    old_location="../ci/",
)
