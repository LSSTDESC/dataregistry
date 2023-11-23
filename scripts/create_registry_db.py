import os
import sys
import argparse
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, Float
from sqlalchemy import ForeignKey, UniqueConstraint, text
from sqlalchemy.orm import relationship, declarative_base
from dataregistry.db_basic import DbConnection, TableCreator, SCHEMA_VERSION
from dataregistry.db_basic import add_table_row, _insert_provenance

"""
A script to create the default dataregistry schema and the production schema.

Both schemas have the same layout, containing six tables:
    - "dataset"         : Primary table, contains information on the datasets
    - "dataset_alias"   : Table to associate "alias" names to datasets
    - "execution"       : Stores the executions, datasets can be linked to these
    - "execution_alias" : Table to asscociate "alias" names to executions
    - "dependancy"      : Tracks dependencies between datasets
    - "provenance"      : Contains information about the database/schema 
"""

Base = declarative_base()

def _get_ForeignKey_str(schema, table, row):
    if schema is None:
        return f"{table}.{row}"
    else:
        return f"{schema}.{table}.{row}"

def _Provenance(schema):
    """Keeps track of database/schema versions."""

    class_name = f"{schema}_provenance"

    # Rows
    rows = {
        "provenance_id": Column("provenance_id", Integer, primary_key=True),
        "code_version_major": Column("code_version_major", Integer, nullable=False),
        "code_version_minor": Column("code_version_minor", Integer, nullable=False),
        "code_version_patch": Column("code_version_patch", Integer, nullable=False),
        "code_version_suffix": Column("code_version_suffix", String),
        "db_version_major": Column("db_version_major", Integer, nullable=False),
        "db_version_minor": Column("db_version_minor", Integer, nullable=False),
        "db_version_patch": Column("db_version_patch", Integer, nullable=False),
        "git_hash": Column("git_hash", String, nullable=True),
        "repo_is_clean": Column("repo_is_clean", Boolean, nullable=True),
        # update method is always "CREATE" for this script.
        # Alternative could be "MODIFY" or "MIGRATE"
        "update_method": Column("update_method", String(10), nullable=False),
        "schema_enabled_date": Column("schema_enabled_date", DateTime, nullable=False),
        "creator_uid": Column("creator_uid", String(20), nullable=False),
        "comment": Column("comment", String(250)),
    }

    # Table metadata
    meta = {"__tablename__": "provenance", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**rows, **meta})
    return Model


def _Execution(schema):
    """Stores executions, which datasets can be linked to."""

    class_name = f"{schema}_execution"

    # Rows
    rows = {
        "execution_id": Column("execution_id", Integer, primary_key=True),
        "description": Column("description", String),
        "register_date": Column("register_date", DateTime, nullable=False),
        "execution_start": Column("execution_start", DateTime),
        # name is meant to identify the code executed.  E.g., could be pipeline name
        "name": Column("name", String),
        # locale is, e.g. site where code was run
        "locale": Column("locale", String),
        "configuration": Column("configuration", String),
        "creator_uid": Column("creator_uid", String(20), nullable=False),
    }

    # Table metadata
    meta = {"__tablename__": "execution", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**rows, **meta})
    return Model


def _ExecutionAlias(schema):
    """To asscociate an alias to an execution."""

    class_name = f"{schema}_execution_alias"

    # Rows
    rows = {
        "execution_alias_id": Column("execution_alias_id", Integer, primary_key=True),
        "alias": Column(String, nullable=False),
        "execution_id": Column(Integer, ForeignKey(_get_ForeignKey_str(schema, "execution", "execution_id"))),
        "supersede_date": Column(DateTime, default=None),
        "register_date": Column(DateTime, nullable=False),
        "creator_uid": Column(String(20), nullable=False),
    }

    # Table metadata
    meta = {
        "__tablename__": "execution_alias",
        "__table_args__": (
            UniqueConstraint("alias", "register_date", name="execution_u_register"),
            {"schema": schema},
        ),
    }

    Model = type(class_name, (Base,), {**rows, **meta})
    return Model


def _DatasetAlias(schema):
    """To asscociate an alias to a dataset."""

    class_name = f"{schema}_dataset_alias"

    # Rows
    rows = {
        "dataset_alias_id": Column(Integer, primary_key=True),
        "alias": Column(String, nullable=False),
        "dataset_id": Column(Integer, ForeignKey(_get_ForeignKey_str(schema, "dataset", "dataset_id"))),
        "supersede_date": Column(DateTime, default=None),
        "register_date": Column(DateTime, nullable=False),
        "creator_uid": Column(String(20), nullable=False),
    }

    # Table metadata
    meta = {
        "__tablename__": "dataset_alias",
        "__table_args__": (
            UniqueConstraint("alias", "register_date", name="dataset_u_register"),
            {"schema": schema},
        ),
    }

    Model = type(class_name, (Base,), {**rows, **meta})
    return Model


def _Dataset(schema):
    """Primary table, stores dataset information."""

    class_name = f"{schema}_dataset"

    # Rows
    rows = {
        "dataset_id": Column(Integer, primary_key=True),
        "name": Column(String, nullable=False),
        "relative_path": Column(String, nullable=False),
        "version_major": Column(Integer, nullable=False),
        "version_minor": Column(Integer, nullable=False),
        "version_patch": Column(Integer, nullable=False),
        "version_string": Column(String, nullable=False),
        "version_suffix": Column(String),
        "dataset_creation_date": Column(DateTime),
        "is_archived": Column(Boolean, default=False),
        "is_external_link": Column(Boolean, default=False),
        "is_overwritable": Column(Boolean, default=False),
        "is_overwritten": Column(Boolean, default=False),
        "is_valid": Column(Boolean, default=True),  # False if, e.g., copy failed
        # The following are boilerplate, included in all or most tables
        "register_date": Column(DateTime, nullable=False),
        "creator_uid": Column(String(20), nullable=False),
        # Make access_API a string for now, but it could be an enumeration or
        # a foreign key into another table.   Possible values for the column
        # might include "gcr-catalogs", "skyCatalogs"
        "access_API": Column("access_API", String(20)),
        # A way to associate a dataset with a program execution or "run"
        "execution_id": Column(Integer, ForeignKey(_get_ForeignKey_str(schema, "execution", "execution_id"))),
        "description": Column(String),
        "owner_type": Column(String, nullable=False),
        # If ownership_type is 'production', then owner is always 'production'
        # If ownership_type is 'group', owner will be a group name
        # If ownership_type is 'user', owner will be a user name
        "owner": Column(String, nullable=False),
        # To store metadata about the dataset.
        "data_org": Column("data_org", String, nullable=False),
        "nfiles": Column("nfiles", Integer, nullable=False),
        "total_disk_space": Column("total_disk_space", Float, nullable=False),
    }

    # Table metadata
    meta = {
        "__tablename__": "dataset",
        "__table_args__": (
            UniqueConstraint(
                "name", "version_string", "version_suffix", name="dataset_u_version"
            ),
            Index("relative_path", "owner", "owner_type"),
            {"schema": schema},
        ),
    }

    Model = type(class_name, (Base,), {**rows, **meta})
    return Model


def _Dependency(schema, has_production):
    """Links datasets through "dependencies"."""

    class_name = f"{schema}_dependency"

    # Rows
    rows = {
        "dependency_id": Column(Integer, primary_key=True),
        "register_date": Column(DateTime, nullable=False),
        "execution_id": Column(Integer, ForeignKey(_get_ForeignKey_str(schema, "execution", "execution_id"))),
        "input_id": Column(Integer, ForeignKey(_get_ForeignKey_str(schema, "dataset", "dataset_id"))),
    }

    # Add link to production schema.
    if has_production:
        rows["input_id_production"] = Column(
            Integer, ForeignKey(_get_ForeignKey_str("production", "dataset", "dataset_id"))
        )

    #        #if SCHEMA != "production":
    #        #    table1 = relationship('production.dataset', foreign_keys=[input_id_production])

    # Table metadata
    meta = {"__tablename__": "dependency", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**rows, **meta})
    return Model


# The following should be adjusted whenever there is a change to the structure
# of the database tables.
_DB_VERSION_MAJOR = 1
_DB_VERSION_MINOR = 2
_DB_VERSION_PATCH = 0
_DB_VERSION_COMMENT = "Added comment column to Provenance table"

# Parse command line arguments
parser = argparse.ArgumentParser(
    description="""
Creates dataregistry tables in specified schema and connection information (config)""",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--schema",
    help="name of schema to contain tables. Will be created if it doesn't already exist",
    default=f"{SCHEMA_VERSION}",
)
parser.add_argument("--config", help="Path to the data registry config file")
parser.add_argument(
    "--no_production", help="Do not create the production schema", action="store_true"
)
args = parser.parse_args()

# Connect to database
db_connection = DbConnection(args.config, args.schema)

# What schemas are we working with?
if db_connection.dialect != "sqlite":
    SCHEMA_LIST = [args.schema, "production"]
else:
    SCHEMA_LIST = [None]
if args.no_production and "production" in SCHEMA_LIST:
    SCHEMA_LIST.remove("production")

# Create the schemas
for SCHEMA in SCHEMA_LIST:
    if SCHEMA is None:
        continue
    stmt = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"
    with db_connection.engine.connect() as conn:
        conn.execute(text(stmt))
        conn.commit()

# Create the tables
for SCHEMA in SCHEMA_LIST:
    _Dataset(SCHEMA)
    _DatasetAlias(SCHEMA)
    _Dependency(SCHEMA, "production" in SCHEMA_LIST)
    _Execution(SCHEMA)
    _ExecutionAlias(SCHEMA)
    _Provenance(SCHEMA)

# Generate the database
Base.metadata.create_all(db_connection.engine)

# Add initial procenance information
for SCHEMA in SCHEMA_LIST:
    prov_id = _insert_provenance(
        DbConnection(args.config, SCHEMA),
        _DB_VERSION_MAJOR,
        _DB_VERSION_MINOR,
        _DB_VERSION_PATCH,
        "CREATE",
        comment=_DB_VERSION_COMMENT,
    )
