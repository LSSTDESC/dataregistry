import os
import sys

from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine, ownertypeenum
from dataregistry.registrar import Registrar

_lookup = {
    "production": ownertypeenum.production,
    "group": ownertypeenum.group,
    "user": ownertypeenum.user,
}

if os.getenv("DREGS_CONFIG") is None:
    raise Exception("Need to set DREGS_CONFIG env variable")
else:
    DREGS_CONFIG = os.getenv("DREGS_CONFIG")

# Establish connection to database
engine, dialect = create_db_engine(config_file=DREGS_CONFIG)

def _parse_version(version_str):
    """
    Pull out the MAJOR.MINOR.PATCH integers from the version string.

    Parameters
    ----------
    version_str : str
        Format "M.N.P"

    Returns
    -------
    M, N, P : int
        Major, Minor, Patch version integers
    """

    v = version_str.split(".")
    assert len(v) == 3, "Bad version string"

    return int(v[0]), int(v[1]), int(v[2])

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

    new_id = registrar.register_dataset_alias(
        name,
        dataset_id,
    )

    assert new_id is not None, "Trying to create a dataset alias that already exists"
    print(f"Created dataset alias entry with id {new_id}")

    return new_id


def _insert_execution_entry(name, description, owner_type, owner):
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
    )

    assert new_id is not None, "Trying to create a execution that already exists"
    print(f"Created execution entry with id {new_id}")

    return new_id


def _insert_dataset_entry(
    name, relpath, version, owner_type, owner, description, execution_id=None
):
    """
    Wrapper to create dataset entry

    Parameters
    ----------
    name : str
        A name for the dataset
    relpath : str
        Relative path within the data registry to store the data
        Relative to <ROOT>/<owner_type>/<owner>/...
    version : str
        Semantic version string (i.e., M.N.P)
    owner_type : str
        Either "production", "group", "user"
    owner : str
        Dataset owner
    description : str
        Description of dataset
    execution_id : int
        Execution entry related to this dataset

    Returns
    -------
    new_id : int
        The dataset it created for this entry
    """

    # Some defaults over all test datasets
    locale = "NERSC"
    is_overwritable = False
    creation_data = None
    old_location = None
    make_sym_link = False
    is_dummy = True
    version_suffix = None
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
    )

    assert new_id is not None, "Trying to create a dataset that already exists"
    print(f"Created dataset entry with id {new_id}")

    return new_id

# Make some test execution entries.
execution_id_1 = _insert_execution_entry(
    "DESC execution 1", "My first pipeline execution", "user", None
)

# Make some test dataset entries.
# These will be dummy entries, copying no actual data.
dataset_id_1 = _insert_dataset_entry(
    "DESC dataset 1",
    "DESC/datasets/my_first_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first DESC dataset",
    execution_id=execution_id_1,
)

_insert_dataset_entry(
    "DESC dataset 1",
    "DESC/datasets/my_first_dataset_v2_minor_upgrade",
    "minor",
    "user",
    None,
    "This is my first DESC dataset (minor version update)",
)

_insert_dataset_entry(
    "DESC dataset 2",
    "DESC/datasets/my_second_dataset",
    "0.0.1",
    "user",
    None,
    "This is my second DESC dataset",
)

_insert_dataset_entry(
    None,
    "DESC/datasets/my_third_dataset.txt",
    "0.2.1",
    "user",
    None,
    "See if default name is correctly generated",
)

# Make some test dataset alias entries.
_insert_alias_entry(
    "My first alias",
    dataset_id_1,
    "user",
    None
)
