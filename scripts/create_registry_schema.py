import argparse
import pandas as pd
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Index,
    Float,
)
from sqlalchemy import ForeignKey, UniqueConstraint, text
from sqlalchemy.orm import DeclarativeBase
from dataregistry.db_basic import DbConnection, SCHEMA_VERSION
from dataregistry.db_basic import _insert_provenance
from dataregistry.schema import load_schema

"""
A script to create a schema.

Both schemas have the same layout, containing six tables:
    - "dataset"         : Primary table, contains information on the datasets
    - "dataset_alias"   : Table to associate "alias" names to datasets
    - "execution"       : Stores executions, datasets can be linked to these
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
            fk_schema = schema
            if schema_yaml[table][column]["foreign_key_schema"] != "self":
                fk_schema = schema_yaml[table][column]["foreign_key_schema"]

            return_dict[column] = Column(
                column,
                _TYPE_TRANSLATE[schema_yaml[table][column]["type"]],
                ForeignKey(
                    _get_ForeignKey_str(
                        fk_schema,
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
    Get the string reference to the "<shema>.<table>.<column>" a foreign key
    will point to.

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
            UniqueConstraint("alias",
                             "register_date",
                             name="execution_u_register"),
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
            UniqueConstraint("alias",
                             "register_date",
                             name="dataset_u_register"),
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
                "name",
                "version_string",
                "version_suffix",
                name="dataset_u_version"
            ),
            Index("relative_path", "owner", "owner_type"),
            {"schema": schema},
        ),
    }

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


def _Dependency(schema, has_production, production="production"):
    """
    Links datasets through "dependencies".

    Parameters
    ----------
    schema          str      Name of schema we're writing to
    has_production  boolean  True if this schema refers to production schema
    production      string   Name of production schema
    """

    class_name = f"{schema}_dependency"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "dependency")

    # Remove link to production schema if unneeded.
    if not has_production:
        del columns["input_production_id"]

    # Update production schema name
    else:
        if production != "production":
            old_col = columns["input_production_id"]
            fkey = ForeignKey(f"{production}.dataset.dataset_id")
            new_input_production_id = Column(old_col.name,
                                             old_col.type,
                                             fkey)
            del columns["input_production_id"]
            columns["input_production_id"] = new_input_production_id

    # Table metadata
    meta = {"__tablename__": "dependency",
            "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model


# The following should be adjusted whenever there is a change to the structure
# of the database tables.
_DB_VERSION_MAJOR = 2
_DB_VERSION_MINOR = 2
_DB_VERSION_PATCH = 0
_DB_VERSION_COMMENT = "Add `location_type` for dataset table"

# Parse command line arguments
parser = argparse.ArgumentParser(
    description="""
Creates dataregistry tables for specified schema and connection information (config)""",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--schema",
    help="name of schema to contain tables. Will be created if it doesn't already exist",
    default=f"{SCHEMA_VERSION}",
)
parser.add_argument(
    "--production-schema", default="production",
    help="name of schema containing production tables.",
)
parser.add_argument("--config", help="Path to the data registry config file")

args = parser.parse_args()
schema = args.schema
prod_schema = args.production_schema

# Connect to database to find out what the backend is
db_connection = DbConnection(args.config, schema)
if db_connection.dialect == "sqlite":
    if schema == "production":
        raise ValueError("Production not available for sqlite databases")
else:
    if schema != prod_schema:
        # production schema, tables must already exists and schema
        # major and minor versions must match
        stmt = f"select db_version_major, db_version_minor from {prod_schema}.provenance order by provenance_id desc limit 1"
        try:
            with db_connection.engine.connect() as conn:
                result = conn.execute(text(stmt))
                result = pd.DataFrame(result)
                conn.commit()
        except Exception:
            raise RuntimeError("production schema does not exist or is ill-formed")
        if result["db_version_major"][0] != _DB_VERSION_MAJOR | int(result["db_version_minor"][0]) > _DB_VERSION_MINOR:
            raise RuntimeError("production schema version incompatible")

stmt = f"CREATE SCHEMA IF NOT EXISTS {schema}"
with db_connection.engine.connect() as conn:
    conn.execute(text(stmt))
    conn.commit()

# Grant reg_reader access
try:
    with db_connection.engine.connect() as conn:
        # Grant reg_reader access.
        acct = "reg_reader"
        usage_prv = f"GRANT USAGE ON SCHEMA {schema} to {acct}"
        select_prv = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} to {acct}"
        conn.execute(text(usage_prv))
        conn.execute(text(select_prv))

        if schema == prod_schema:      # also grant privileges to reg_writer
            acct = "reg_writer"
            usage_priv = f"GRANT USAGE ON SCHEMA {schema} to {acct}"
            select_priv = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} to {acct}"
            conn.execute(text(usage_priv))
            conn.execute(text(select_priv))
            conn.commit()
except Exception as e:
    raise e(f"Could not grant access to {acct} on schema {schema}")

# Create the tables
# for SCHEMA in SCHEMA_LIST:
_Dataset(schema)
_DatasetAlias(schema)
_Dependency(schema, db_connection.dialect != "sqlite",
            production=prod_schema)
_Execution(schema)
_ExecutionAlias(schema)
_Provenance(schema)

# Generate the database
if schema != prod_schema:
    Base.metadata.reflect(db_connection.engine, prod_schema)
Base.metadata.create_all(db_connection.engine)

# Add initial provenance information
prov_id = _insert_provenance(
    DbConnection(args.config, schema),
    _DB_VERSION_MAJOR,
    _DB_VERSION_MINOR,
    _DB_VERSION_PATCH,
    "CREATE",
    comment=_DB_VERSION_COMMENT,
)
