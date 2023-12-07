from sqlalchemy import engine_from_config
from sqlalchemy.engine import make_url
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import column, text, insert, select
from sqlalchemy.exc import DBAPIError, IntegrityError
import yaml
import os
from datetime import datetime
from collections import namedtuple
from dataregistry import __version__
from dataregistry.git_util import get_git_info
from git import InvalidGitRepositoryError

"""
Low-level utility routines and classes for accessing the registry
"""

SCHEMA_VERSION = "registry_beta"

__all__ = [
    "DbConnection",
    "add_table_row",
    "TableMetadata",
    "SCHEMA_VERSION",
]


def _get_dataregistry_config(config_file=None, verbose=False):
    """
    Locate the data registry configuration file.

    The code will check three scenarios, which are, in order of priority:
        - The config_file has been manually passed
        - The DATAREG_CONFIG env variable has been set
        - The default location (the .config_reg_access file in $HOME)

    If none of these are true, an exception is raised.

    Parameters
    ----------
    config_file : str, optional
        Manually set the location of the config file
    verbose : bool, optional
        True for more output

    Returns
    -------
    config_file : str
        Path to data registry configuration file
    """

    _default_loc = os.path.join(os.getenv("HOME"), ".config_reg_access")

    # Case where the user has manually specified the location
    if config_file is not None:
        if verbose:
            print(f"Using manually passed config file ({config_file})")
        return config_file

    # Case where the env variable is set
    elif os.getenv("DATAREG_CONFIG"):
        if verbose:
            print(
                f"Using DATAREG_CONFIG env var for config file",
                f"({os.getenv('DATAREG_CONFIG')})",
            )
        return os.getenv("DATAREG_CONFIG")

    # Finally check default location in $HOME
    elif os.path.isfile(_default_loc):
        if verbose:
            print("Using default location for config file", f"({_default_loc})")
        return _default_loc
    else:
        raise ValueError("Unable to located data registry config file")


def add_table_row(conn, table_meta, values, commit=True):
    """
    Generic insert, given connection, metadata for a table and column values to
    be used.

    Parameters
    ----------
    conn : SQLAlchemy Engine object
        Connection to the database
    table_meta : TableMetadata object
        Table we are inserting data into
    values : dict
        Properties to be entered
    commit : bool, optional
        True to commit changes to database (default True)

    Returns
    -------
    - : int
        Primary key for new row if successful
    """

    result = conn.execute(insert(table_meta), [values])

    if commit:
        conn.commit()

    return result.inserted_primary_key[0]


class DbConnection:
    def __init__(self, config_file=None, schema=None, verbose=False):
        """
        Simple class to act as container for connection

        Parameters
        ----------
        config : str, optional
            Path to config file with low-level connection information.
            If None, default location is assumed
        schema : str, optional
            Schema to connect to.  If None, default schema is assumed
        verbose : bool, optional
            If True, produce additional output
        """

        # Extract connection info from configuration file
        with open(_get_dataregistry_config(config_file, verbose)) as f:
            connection_parameters = yaml.safe_load(f)

        # Build the engine
        self._engine = engine_from_config(connection_parameters)

        # Pull out the working schema version
        driver = make_url(connection_parameters["sqlalchemy.url"]).drivername
        self._dialect = driver.split("+")[0]

        if self._dialect == "sqlite":
            self._schema = None
        else:
            if schema is None:
                self._schema = SCHEMA_VERSION
            else:
                self._schema = schema

    @property
    def engine(self):
        return self._engine

    @property
    def dialect(self):
        return self._dialect

    @property
    def schema(self):
        return self._schema


class TableMetadata:
    def __init__(self, db_connection, get_db_version=True):
        """
        Keep and dispense table metadata

        Parameters
        ----------
        db_connection : DbConnection object
            Stores information about the DB connection
        get_db_version : bool, optional
            True to extract the DB version from the provinance table
        """

        self._metadata = MetaData(schema=db_connection.schema)
        self._engine = db_connection.engine
        self._schema = db_connection.schema

        # Load all existing tables
        self._metadata.reflect(self._engine, db_connection.schema)

        # Fetch and save db versioning if present and requested
        if db_connection.dialect == "sqlite":
            prov_name = "provenance"
        else:
            prov_name = ".".join([self._schema, "provenance"])
        if prov_name in self._metadata.tables and get_db_version:
            prov_table = self._metadata.tables[prov_name]
            cols = ["db_version_major", "db_version_minor", "db_version_patch"]
            stmt = select(*[column(c) for c in cols])
            stmt = stmt.select_from(prov_table)
            stmt = stmt.order_by(prov_table.c.provenance_id.desc())
            with self._engine.connect() as conn:
                results = conn.execute(stmt)
                conn.commit()
            r = results.fetchone()
            self._db_major = r[0]
            self._db_minor = r[1]
            self._db_patch = r[2]
        else:
            self._db_major = None
            self._db_minor = None
            self._db_patch = None

    @property
    def db_version_major(self):
        return self._db_major

    @property
    def db_version_minor(self):
        return self._db_minor

    @property
    def db_version_patch(self):
        return self._db_patch

    def get(self, tbl):
        if "." not in tbl:
            if self._schema:
                tbl = ".".join([self._schema, tbl])
        if tbl not in self._metadata.tables.keys():
            try:
                self._metadata.reflect(self._engine, only=[tbl])
            except:
                raise ValueError(f"No such table {tbl}")

        return self._metadata.tables[tbl]


def _insert_provenance(
    db_connection,
    db_version_major,
    db_version_minor,
    db_version_patch,
    update_method,
    comment=None,
):
    """
    Write a row to the provenance table. Includes version of db schema,
    version of code, etc.

    Parameters
    ----------
    db_version_major : int
    db_version_minor : int
    db_version_patch : int
    update_method : str
        One of "create", "migrate"
    comment : str, optional
        Briefly describe reason for new version

    Returns
    -------
    id : int
        Id of new row in provenance table
    """
    version_fields = __version__.split(".")
    patch = version_fields[2]
    suffix = None
    if "-" in patch:
        subfields = patch.split("-")
        patch = subfields[0]
        suffix = "-".join(subfields[1:])

    values = dict()
    values["code_version_major"] = version_fields[0]
    values["code_version_minor"] = version_fields[1]
    values["code_version_patch"] = patch
    if suffix:
        values["code_version_suffix"] = suffix
    values["db_version_major"] = db_version_major
    values["db_version_minor"] = db_version_minor
    values["db_version_patch"] = db_version_patch
    values["schema_enabled_date"] = datetime.now()
    values["creator_uid"] = os.getenv("USER")
    pkg_root = os.path.join(os.path.dirname(__file__), "../..")

    # If this is a git repo, save hash and state
    try:
        git_hash, is_clean = get_git_info(pkg_root)
        values["git_hash"] = git_hash
        values["repo_is_clean"] = is_clean
    except InvalidGitRepositoryError as e:
        # no git repo; this is an install. Code version is sufficient
        pass

    values["update_method"] = update_method
    if comment is not None:
        values["comment"] = comment

    prov_table = TableMetadata(db_connection, get_db_version=False).get("provenance")
    with db_connection.engine.connect() as conn:
        id = add_table_row(conn, prov_table, values)

        return id
