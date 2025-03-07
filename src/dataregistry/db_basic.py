from sqlalchemy import engine_from_config
from sqlalchemy.engine import make_url
from sqlalchemy import MetaData
from sqlalchemy import column, insert, select
import yaml
import os
import logging
from datetime import datetime
from dataregistry import __version__
from dataregistry.exceptions import DataRegistryException
from dataregistry.schema import DEFAULT_NAMESPACE
from functools import cached_property

"""
Low-level utility routines and classes for accessing the registry
"""

__all__ = [
    "DbConnection",
    "add_table_row",
]


def _get_dataregistry_config(logger, config_file=None):
    """
    Locate the data registry configuration file.

    The code will check three scenarios, which are, in order of priority:
        - The config_file has been manually passed
        - The DATAREG_CONFIG env variable has been set
        - The default location (the .config_reg_access file in $HOME)

    If none of these are true, an exception is raised.

    Parameters
    ----------
    logger : logging object
    config_file : str, optional
        Manually set the location of the config file

    Returns
    -------
    config_file : str
        Path to data registry configuration file
    """

    _default_loc = os.path.join(os.getenv("HOME"), ".config_reg_access")

    # Case where the user has manually specified the location
    if config_file is not None:
        logger.debug(f"Using manually passed config file ({config_file})")
        return config_file

    # Case where the env variable is set
    elif os.getenv("DATAREG_CONFIG"):
        logger.debug(
            (
                f"Using DATAREG_CONFIG env var for config file "
                f"({os.getenv('DATAREG_CONFIG')})"
            )
        )
        return os.getenv("DATAREG_CONFIG")

    # Finally check default location in $HOME
    elif os.path.isfile(_default_loc):
        logger.debug(f"Using default location for config file ({_default_loc})")
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
    def __init__(
        self,
        namespace=None,
        config_file=None,
        schema=None,
        logging_level=logging.INFO,
        entry_mode="working",
        query_mode="both",
        creation_mode=False,
    ):
        """
        Simple class to act as container for connection.

        The DESC dataregistry internals always expect a working/production
        schema pairing (except in the case of sqlite where there is only a
        single "database" and no concept of schemas). Here the `schema` passed
        is the working schema name, the production schema associated with that
        working schema is automatially deduced via the `provenance` table. Both
        the working and production schemas are connected to and reflected here.

        The `schema` passed to this function should always be the working
        schema, the only exception is during schema creation, see note below.

        Connection modes
        ----------------
        `namespace=` :
            Connects to a "namespace", which is a pairing of a "working" and
            "production" schema, referred to jointly as a namespace. During
            queries, by default entries from both schemas are searched and
            their results combined (this behaviour can be changed using the
            `query_mode` option). When creating new entries in the
            dataregistry, or when modifying or deleting previous entries, the
            `entry_mode` schema is used (which is the "working" schema by
            default).
        `schema=` :
            Connects directly to the chosen schema (by full name, e.g.,
            "<namespace>_working"). Queries are limited to that individual
            schema, and new entries/modifications can only go into this schema.
            This connection mode is generally for schema creation, or testing.

        Parameters
        ----------
        namespace : str, optional
            Namespace to connect to. If None, the default namespace will be
            used.
        config_file : str
            Path to config file, if None, default location is assumed.
        schema : str, optional
            Schema to connect to, to connect directly to a chosen schema,
            bypassing the namespace (creation of schemas or testing purposes only).
        logging_level : int, optional
            Level for the logger output (default is logging.INFO)
        entry_mode : str, optional
            Which schema ("working" or "production") within the namespace to
            write new (or modify/delete previous) entries to. This defines the
            `entry_schema` in the connection object.
        query_mode : str, optional
            When querying, both the working and production schemas within the
            namespace are jointly searched and their results combined
            (`query_mode`="both"). However setting `query_mode` to either
            "working" or "production" will restrict queries to only the chosen
            schema.
        creation_mode : bool
            During schema creation the database cannot be "reflected" (as it
            does not exist yet). This flag prevents reflecting a current
            database.  When in creation mode, do not pass a namespace, instead
            directly pass the schema name which you are creating.
        """

        # Set up logger
        self._setup_logger(logging_level)

        # Extract connection info from configuration file
        with open(_get_dataregistry_config(self.logger, config_file)) as f:
            connection_parameters = yaml.safe_load(f)

        # Build the engine
        self._engine = engine_from_config(connection_parameters)

        # Pull out the database dialect
        driver = make_url(connection_parameters["sqlalchemy.url"]).drivername
        self._dialect = driver.split("+")[0]

        # Make sure manually passed schema name is valid formatting
        # If `schema` is passed, it also sets the entry and query modes
        if schema is not None:
            schema_type = schema.split("_")[-1]
            if schema_type not in ["working", "production"]:
                raise ValueError(f"Invalid schema name {schema}, {schema_type} not valid type")
            query_mode, entry_mode = schema_type, schema_type
            namespace = None

        # Define working schema from the namespace, or manually
        if self._dialect == "sqlite":
            self._schema = None
            self._namespace = None
        else:
            if schema is None:
                if namespace is None:
                    self._schema = DEFAULT_NAMESPACE + "_working"
                    self._namespace = DEFAULT_NAMESPACE
                else:
                    self._schema = namespace + "_working"
                    self._namespace = namespace
            else:
                self._schema = schema
                self._namespace = None

        # Check `cretion_mode` is allowed
        if creation_mode and schema is None:
            raise DataRegistryException(
                "`creation_mode` can only be flagged when passing a `schema`"
            )

        # Namespace schema must be either "working" or "production"
        if entry_mode not in ["working", "production"]:
            raise ValueError("`entry_mode` must be either working or production")

        # Query mode can only be "both", "working" or "production"
        if query_mode not in ["both", "working", "production"]:
            raise ValueError("`query_mode` must be 'both', 'working' or 'production'")

        # Dict to store schema/table information (filled in `_reflect()`)
        self.metadata = {}
        self._creation_mode = creation_mode

        # What schema do new entries go into?
        self._entry_mode = entry_mode

        # Which schemas are queried?
        self._query_mode = query_mode

        # Report connection
        self.logger.debug(f"database type is {self._dialect}")
        self.logger.debug(f"Connected to namespace:{namespace} schema:{schema}")
        self.logger.debug(f"creation_mode:{creation_mode}")
        self.logger.debug(f"entry_mode:{entry_mode} query_mode:{entry_mode}")

    @property
    def namespace(self):
        return self._namespace

    @property
    def engine(self):
        return self._engine

    @property
    def dialect(self):
        return self._dialect

    @property
    def schema(self):
        # When working within a namespace, this is the "working" schema
        return self._schema

    @property
    def production_schema(self):
        # Database hasn't been reflected yet
        if len(self.metadata) == 0:
            self._reflect()

        # When working within a namespace, this is the "production" schema
        return self._prod_schema

    @property
    def entry_schema(self):
        """
        Which schema (working or production) is being used for new entries,
        modification, or deletions.
        """

        # sqlite case
        if self.namespace is None:
            return self.schema

        # Which is the entry schema
        else:
            if self._entry_mode == "production":
                return self.production_schema
            else:
                return self.schema

    @property
    def entry_schema_is_production(self):
        """Is the entry schema a production schema?"""
        if self.dialect == "sqlite":
            return False
        else:
            return "_production" in self.entry_schema

    def _setup_logger(self, logging_level):
        """
        Set up the reporting logger

        Parameters
        ----------
        logging_level : int
        """

        # Configure the logging system
        logging.basicConfig(
            level=logging_level,  # Set the threshold for which messages are processed
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        )
        
        # Create a logger object
        self.logger = logging.getLogger(__name__)

    def _reflect(self):
        """
        Reflect the working and production schemas to get the tables within the
        database.

        When the connection is defined by a namespace (i.e., we haven't
        specified a single schema to connect to), the production schema is
        automatically derived from the working schema through the provenance
        table. The tables and versions of each schema are extracted and stored
        in the `self.metadata` dict.
        """

        self.logger.debug("Reflecting database")

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
            self.logger.debug(f"Executing {stmt}")
            with self.engine.connect() as conn:
                results = conn.execute(stmt)
                r = results.fetchone()
            if r is None:
                raise DataRegistryException(
                    "During reflection no provenance information was found"
                )

            if get_associated_production:
                return f"{r[0]}.{r[1]}.{r[2]}", r[3]
            else:
                return f"{r[0]}.{r[1]}.{r[2]}", None

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

        # From the provenance table get the associated production schema
        prov_table = metadata.tables[prov_name]
        if self._creation_mode:
            self.metadata["schema_version"], self._prod_schema = None, None
        else:
            self.metadata["schema_version"], self._prod_schema = _get_db_info(
                prov_table, get_associated_production=(True if self.namespace else False) 
            )

        # Don't go on to query the provenance table unless working within a namespace
        if self.namespace is None:
            self.metadata["tables"] = metadata.tables
            return

        # Add production schema tables to metadata
        if self.dialect != "sqlite":
            metadata.reflect(self.engine, self._prod_schema)
            prov_name = ".".join([self._prod_schema, "provenance"])
            prov_table = metadata.tables[prov_name]
            self.metadata["prod_schema_version"], _ = _get_db_info(prov_table)
        else:
            self.metadata["prod_schema_version"] = None

        # Store metadata
        self.metadata["tables"] = metadata.tables

        # Report metadata
        for att, v in self.metadata.items():
            if att == "tables":
                continue
            self.logger.debug(f"Table metadata: {att} - {v}")

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
        all_columns = set()
        for table in self.metadata["tables"]:
            for column in self.metadata["tables"][table].c:
                # Only need to focus on a single schema (due to duplicate layout)
                if self.metadata["tables"][table].schema != self.entry_schema:
                    continue

                if column.name in all_columns:
                    duplicates.add(column.name)
                all_columns.add(column.name)

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
            If `None`, the `entry_schema` is used

        Returns
        -------
        - : SqlAlchemy Metadata object
        """

        # Database hasn't been reflected yet
        if len(self.metadata) == 0:
            self._reflect()

        # Which schema to get the table from
        if schema is None:
            schema = self.entry_schema

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

    # Report values
    db_connection.logger.debug("Inserting new provenance information")
    for att, v in values.items():
        db_connection.logger.debug(f"  - {att}: {v}")

    # Add values
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

    Keywords are always ingested as lower case.

    Parameters
    ----------
    db_connection : DbConnection class
        Conenction to the database
    keyword : str
        Keyword to add (added in lower case form regardless of input)
    system : bool
        True if this is a preset system keyword (False for user custom keyword)
    creator_uid : int, optional

    Returns
    -------
    id : int
        Id of new row in keyword table
    """

    if not isinstance(keyword, str):
        print(f"Only string keywords can be inserted")
        return

    values = dict()
    values["keyword"] = keyword.lower()
    values["system"] = system
    if creator_uid is None:
        values["creator_uid"] = os.getenv("USER")
    else:
        values["creator_uid"] = creator_uid
    values["creation_date"] = datetime.now()
    values["active"] = True

    # Report new keyword
    db_connection.logger.debug("Inserting new keyword")
    for att, v in values.items():
        db_connection.logger.debug(f"  - {att}: {v}")

    # Add keyword
    keyword_table = db_connection.get_table("keyword")
    with db_connection.engine.connect() as conn:
        id = add_table_row(conn, keyword_table, values)

        return id
