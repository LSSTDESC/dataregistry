import os
import sys
import argparse
from datetime import datetime
from sqlalchemy import (
    Column,
    ColumnDefault,
    Integer,
    String,
    DateTime,
    Boolean,
    Index,
    Float,
)
from sqlalchemy import ForeignKey, UniqueConstraint, text
from sqlalchemy.orm import relationship, DeclarativeBase
from dataregistry.db_basic import DbConnection, SCHEMA_VERSION
from dataregistry.db_basic import _insert_provenance
from dataregistry.schema import load_schema

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

# Conversion from string types in `schema.yaml` to SQLAlchemy
_TYPE_TRANSLATE = {
    "String": String,
    "Integer": Integer,
    "DateTime": DateTime,
    "StringShort": String(20),
    "StringLong": String(250),
    "Boolean": Boolean,
    "Float": Float,
}

# Load the schema from the `schema.yaml` file
schema_yaml = load_schema()


def _get_column_definitions(schema, table):
    """
    Build the SQLAlchemy `Column` list for this table from the information in
    the `schema.yaml` file.

    Parameters
    ----------
    schema : str
    table : str

    Returns
    -------
    return_dict : dict
        SQLAlchemy Column entries for each table
    """

    return_dict = {}
    for column in schema_yaml[table].keys():
        # Special case where column has a foreign key
        if schema_yaml[table][column]["foreign_key"]:
            if schema_yaml[table][column]["foreign_key_schema"] == "self":
                schema_yaml[table][column]["foreign_key_schema"] = schema

            return_dict[column] = Column(
                column,
                _TYPE_TRANSLATE[schema_yaml[table][column]["type"]],
                ForeignKey(
                    _get_ForeignKey_str(
                        schema_yaml[table][column]["foreign_key_schema"],
                        schema_yaml[table][column]["foreign_key_table"],
                        schema_yaml[table][column]["foreign_key_column"],
                    )
                ),
                primary_key=schema_yaml[table][column]["primary_key"],
                nullable=schema_yaml[table][column]["nullable"],
            )

        # Normal case
        else:
            return_dict[column] = Column(
                column,
                _TYPE_TRANSLATE[schema_yaml[table][column]["type"]],
                primary_key=schema_yaml[table][column]["primary_key"],
                nullable=schema_yaml[table][column]["nullable"],
            )

    return return_dict


class Base(DeclarativeBase):
    pass


def _get_ForeignKey_str(schema, table, column):
    """
    Get the string reference to the "<shema>.<table>.<column>" a foreign key will
    point to.

    The schema address will only be included for postgres backends.

    Parameters
    ---------
    schema : str
    table : str
    column : str

    Returns
    -------
    - : str
    """

    if schema is None:
        return f"{table}.{column}"
    else:
        return f"{schema}.{table}.{column}"


def _Provenance(schema):
    """Keeps track of database/schema versions."""

    class_name = f"{schema}_provenance"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "provenance")

    # Table metadata
    meta = {"__tablename__": "provenance", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


def _Execution(schema):
    """Stores executions, which datasets can be linked to."""

    class_name = f"{schema}_execution"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "execution")

    # Table metadata
    meta = {"__tablename__": "execution", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


def _ExecutionAlias(schema):
    """To asscociate an alias to an execution."""

    class_name = f"{schema}_execution_alias"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "execution_alias")

    # Table metadata
    meta = {
        "__tablename__": "execution_alias",
        "__table_args__": (
            UniqueConstraint("alias", "register_date", name="execution_u_register"),
            {"schema": schema},
        ),
    }

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


def _DatasetAlias(schema):
    """To asscociate an alias to a dataset."""

    class_name = f"{schema}_dataset_alias"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "dataset_alias")

    # Table metadata
    meta = {
        "__tablename__": "dataset_alias",
        "__table_args__": (
            UniqueConstraint("alias", "register_date", name="dataset_u_register"),
            {"schema": schema},
        ),
    }

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


def _Dataset(schema):
    """Primary table, stores dataset information."""

    class_name = f"{schema}_dataset"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "dataset")

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

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


def _Dependency(schema, has_production):
    """Links datasets through "dependencies"."""

    class_name = f"{schema}_dependency"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "dependency")

    # Remove link to production schema.
    if not has_production:
        del columns["input_production_id"]

    # Table metadata
    meta = {"__tablename__": "dependency", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


# The following should be adjusted whenever there is a change to the structure
# of the database tables.
_DB_VERSION_MAJOR = 2
_DB_VERSION_MINOR = 2
_DB_VERSION_PATCH = 0
_DB_VERSION_COMMENT = "Add locale for dataset table"

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
    with db_connection.engine.connect() as conn:
        # Create schema
        stmt = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"
        conn.execute(text(stmt))
        conn.commit()

# Grant reg_reader access
acct = "reg_reader"
for SCHEMA in SCHEMA_LIST:
    if SCHEMA is None:
        continue
    try:
        with db_connection.engine.connect() as conn:
            # Grant reg_reader access
            usage_priv = f"GRANT USAGE ON SCHEMA {SCHEMA} to {acct}"
            select_priv = f"GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA} to {acct}"
            conn.execute(text(usage_priv))
            conn.execute(text(select_priv))

            conn.commit()
    except:
        print(f"Could not grant access to {acct} on schema {SCHEMA}")

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

for SCHEMA in SCHEMA_LIST:
    # Add initial procenance information
    prov_id = _insert_provenance(
        DbConnection(args.config, SCHEMA),
        _DB_VERSION_MAJOR,
        _DB_VERSION_MINOR,
        _DB_VERSION_PATCH,
        "CREATE",
        comment=_DB_VERSION_COMMENT,
    )
