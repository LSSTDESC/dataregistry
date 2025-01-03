from sqlalchemy import engine_from_config
from sqlalchemy.engine import make_url
from sqlalchemy import MetaData
from sqlalchemy import column,  insert, select
import yaml
import os
from datetime import datetime
from dataregistry import __version__
from dataregistry.exceptions import DataRegistryException
from dataregistry.schema import DEFAULT_SCHEMA_WORKING
from functools import cached_property

"""
Low-level utility routines and classes for accessing the registry
"""

__all__ = [
    "DbConnection",
    "add_table_row",
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
                "Using DATAREG_CONFIG env var for config file",
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
    table_meta : SqlAlchemy Metadata object
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
    def __init__(self, config_file=None, schema=None, verbose=False, production_mode=False, creation_mode=False):
        """
        Simple class to act as container for connection

        Special cases
        -------------
        production_mode :
            By default a connection to the working schema will be made, and
            from this the paired production schema will be deduced from the
            provenance table. In the default mode both schemas
            working/production are avaliable for queries, but new
            entries/modifications are done to the working schema. To create new
            entries/modifications to production entries, `production_mode` must
            be `True`.
        creation_mode :
            During schema creation, the working/production schema pairs are yet
            to be created. This flag has to be changed to `True` during schema
            creation to skip querying the provenance table for information. In
            this mode the passed `schema` can either be the working or
            production schema name.  

        Parameters
        ----------
        config : str, optional
            Path to config file with low-level connection information.
            If None, default location is assumed
        schema : str, optional
            Working schema to connect to.  If None, default working schema is
            assumed
        verbose : bool, optional
            If True, produce additional output
        production_mode : bool, optional
            True to register/modify production schema entries
        """

        # Extract connection info from configuration file
        with open(_get_dataregistry_config(config_file, verbose)) as f:
            connection_parameters = yaml.safe_load(f)

        # Build the engine
        self._engine = engine_from_config(connection_parameters)

        # Pull out the database dialect
        driver = make_url(connection_parameters["sqlalchemy.url"]).drivername
        self._dialect = driver.split("+")[0]

        # Define working schema
        if self._dialect == "sqlite":
            self._schema = None
        else:
            if schema is None:
                self._schema = DEFAULT_SCHEMA_WORKING
            else:
                self._schema = schema

        # Dict to store schema/table information (filled in `_reflect()`)
        self.metadata = {}

        # Are we working in production mode for this instance?
        self._production_mode = production_mode

        # Are we in schema creation mode?
        self._creation_mode = creation_mode

    @property
    def engine(self):
        return self._engine

    @property
    def dialect(self):
        return self._dialect

    @property
    def schema(self):
        return self._schema

    @property
    def production_schema(self):
        # Database hasn't been reflected yet
        if len(self.metadata) == 0:
            self._reflect()

        return self._prod_schema

    @property
    def active_schema(self):
        if self._production_mode:
            return self._prod_schema
        else:
            return self._schema

    @property
    def production_mode(self):
        return self._production_mode

    @property
    def creation_mode(self):
        return self._creation_mode

    def _reflect(self):
        """
        Reflect the working and production schemas to get the tables within the
        database.

        When the connection is *not* in `creation_mode` (which is the default),
        the production schema is automatically derived from the working schema
        through the provenance table. The tables and versions of each schema
        are extracted and stored in the `self.metadata` dict.
        """

        def _get_db_info(prov_table, get_associated_production=False):
            """
            Get provenance information (version and associated production
            schema) from provenance table.

            Parameters
            ----------
            prov_table : SqlAlchemy metadata
            get_associated_production : bool, optional

            Returns
            -------
            schema_version : str
            associated_production schema : str
                If get_associated_production=True
            """

            # Columns to query
            cols = ["db_version_major", "db_version_minor", "db_version_patch"]
            if get_associated_production:
                cols.append("associated_production")
        
            # Execute query
            stmt = select(*[column(c) for c in cols]).select_from(prov_table)
            stmt = stmt.order_by(prov_table.c.provenance_id.desc())
            with self.engine.connect() as conn:
                results = conn.execute(stmt)
                r = results.fetchone()
            if r is None:
                raise DataRegistryException(
                    "During reflection no provenance information was found")

            if get_associated_production:
                return f"{r[0]}.{r[1]}.{r[2]}", r[3]
            else:
                return f"{r[0]}.{r[1]}.{r[2]}"

        # Reflect the working schema to find database tables
        metadata = MetaData(schema=self.schema)
        metadata.reflect(self.engine, self.schema)

        # Find the provenance table in the working schema
        if self.dialect == "sqlite":
            prov_name = "provenance"
        else:
            prov_name = ".".join([self.schema, "provenance"])

        if prov_name not in metadata.tables:
            raise DataRegistryException(
                f"Incompatible database: no Provenance table {prov_name}, "
                f"listed tables are {metadata.tables}"
                )

        # Don't go on to query the provenance table during schema creation
        if self.creation_mode:
            self.metadata["tables"] = metadata.tables
            return

        # From the procenance table get the associated production schema
        prov_table = metadata.tables[prov_name]
        self.metadata["schema_version"], self._prod_schema = _get_db_info(prov_table, get_associated_production=True)

        # Add production schema tables to metadata
        if self.dialect != "sqlite":
            metadata.reflect(self.engine, self._prod_schema)
            prov_name = ".".join([self._prod_schema, "provenance"])
            prov_table = metadata.tables[prov_name]
            self.metadata["prod_schema_version"] = _get_db_info(prov_table)
        else:
            self.metadata["prod_schema_version"] = None

        # Store metadata
        self.metadata["tables"] = metadata.tables

    @cached_property
    def duplicate_column_names(self):
        """
        Probe the database for tables which share column names. This is used
        later for querying.

        Returns
        -------
        duplicates : list
            List of column names that are duplicated across tables
        """

        # Database hasn't been reflected yet
        if len(self.metadata) == 0:
            self._reflect()

        # Find duplicate column names
        duplicates = set()
        all_columns = []
        for table in self.metadata["tables"]:
            for column in self.metadata["tables"][table].c:
                if self.metadata["tables"][table].schema != self.active_schema:
                    continue

                if column.name in all_columns:
                    duplicates.add(column.name)
                all_columns.append(column.name)

        return list(duplicates)

    def get_table(self, tbl, schema=None):
        """
        Get metadata for a specific table in the database.

        This looks for the table within the `self.metadata` dict. If the dict
        is empty, i.e., this is is the first call in this instance, the
        database is reflected first.

        Parameters
        ----------
        tbl : str
            Name of table we want metadata for
        schema : bool, optional
            Which schema to get the table from
            If `None`, the `active_schema` is used

        Returns
        -------
        - : SqlAlchemy Metadata object
        """

        # Database hasn't been reflected yet
        if len(self.metadata) == 0:
            self._reflect()

        # Which schema to get the table from
        if schema is None:
            schema = self.active_schema

        # Find table
        if "." not in tbl:
            if schema:
                tbl = ".".join([schema, tbl])
        if tbl not in self.metadata["tables"].keys():
            raise ValueError(f"No such table {tbl}")
        return self.metadata["tables"][tbl]


def _insert_provenance(
    db_connection,
    db_version_major,
    db_version_minor,
    db_version_patch,
    update_method,
    comment=None,
    associated_production="production",
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
    associated_production : str, defaults to "production"
        Name of production schema, if any, this schema may reference

    Returns
    -------
    id : int
        Id of new row in provenance table
    """
    from dataregistry.git_util import get_git_info
    from git import InvalidGitRepositoryError

    version_fields = __version__.split(".")
    values = dict()
    values["code_version_major"] = version_fields[0]
    values["code_version_minor"] = version_fields[1]
    values["code_version_patch"] = version_fields[2]
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
    except InvalidGitRepositoryError:
        # no git repo; this is an install. Code version is sufficient
        pass

    values["update_method"] = update_method
    if comment is not None:
        values["comment"] = comment
    if associated_production is not None:  # None is normal for sqlite
        values["associated_production"] = associated_production
    prov_table = db_connection.get_table("provenance")
    with db_connection.engine.connect() as conn:
        id = add_table_row(conn, prov_table, values)

        return id

def _insert_keyword(
    db_connection,
    keyword,
    system,
    creator_uid=None,
):
    """
    Write a row to a keyword table.

    Parameters
    ----------
    db_connection : DbConnection class
        Conenction to the database
    keyword : str
        Keyword to add
    system : bool
        True if this is a preset system keyword (False for user custom keyword)
    creator_uid : int, optional

    Returns
    -------
    id : int
        Id of new row in keyword table
    """

    values = dict()
    values["keyword"] = keyword
    values["system"] = system
    if creator_uid is None:
        values["creator_uid"] = os.getenv("USER")
    else:
        values["creator_uid"] = creator_uid
    values["creation_date"] = datetime.now()
    values["active"] = True

    keyword_table = db_connection.get_table("keyword")
    with db_connection.engine.connect() as conn:
        id = add_table_row(conn, keyword_table, values)

        return id
