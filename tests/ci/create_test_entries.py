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
    relpath, version, owner_type, owner, description, name=None, execution_id=None
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
    name="named_dataset"
)

# Test set 3
# - Test version bumping
_insert_dataset_entry(
    "DESC/datasets/bump_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first bumped DESC dataset",
    name="bump_dataset"
)

_insert_dataset_entry(
    "DESC/datasets/bump_dataset_2",
    "patch",
    "user",
    None,
    "This is my second bumped DESC dataset",
    name="bump_dataset"
)

_insert_dataset_entry(
    "DESC/datasets/bump_dataset_3",
    "minor",
    "user",
    None,
    "This is my third bumped DESC dataset",
    name="bump_dataset"
)

_insert_dataset_entry(
    "DESC/datasets/bump_dataset_4",
    "major",
    "user",
    None,
    "This is my fourth bumped DESC dataset",
    name="bump_dataset"
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

_insert_alias_entry(
    "nice_dataset_name",
    dataset_id,
    "production",
    None
)
