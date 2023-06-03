import os
import sys
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

_lookup = {
    "production": ownertypeenum.production,
    "group": ownertypeenum.group,
    "user": ownertypeenum.user,
}

if os.getenv("DREGS_CONFIG") is None:
    DREGS_CONFIG = "/home/mcalpine/Documents/dataregistry/config_local"
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


def _insert_entry(name, relpath, version, owner_type, owner, description):
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
    """

    # Some defaults over all test datasets
    locale = "NERSC"
    is_overwritable = False
    creation_data = None
    old_location = None
    make_sym_link = False
    schema_version = SCHEMA_VERSION
    is_dummy = True
    version_suffix = ""
    if owner is None:
        owner = os.getenv("USER")

    # Create Registrar object
    registrar = Registrar(
        engine, dialect, _lookup[owner_type], owner=owner, schema_version=schema_version
    )

    # Add new entry.
    v_major, v_minor, v_patch = _parse_version(version)

    new_id = registrar.register_dataset(
        name,
        relpath,
        v_major,
        v_minor,
        v_patch,
        version_suffix=version_suffix,
        creation_date=creation_data,
        description=description,
        old_location=old_location,
        copy=(not make_sym_link),
        is_dummy=is_dummy,
    )

    assert new_id is not None, "Trying to create a dataset that already exists"
    print(f"Created dataset entry with id {new_id}")


# Make some test entries. These will be dummy entries, copying no actual data.
_insert_entry(
    "DESC dataset 1",
    "DESC/datasets/my_first_dataset",
    "0.0.1",
    "user",
    None,
    "This is my first DESC dataset",
)

_insert_entry(
    "DESC dataset 1",
    "DESC/datasets/my_first_dataset",
    "0.0.2",
    "user",
    None,
    "This is my first DESC dataset (updated)",
)

_insert_entry(
    "DESC dataset 2",
    "DESC/datasets/my_second_dataset",
    "0.0.1",
    "user",
    None,
    "This is my second DESC dataset",
)

_insert_entry(
    "DESC production dataset 1",
    "DESC/datasets/my_first_production_dataset",
    "0.0.1",
    "production",
    None,
    "This is my first DESC production dataset",
)