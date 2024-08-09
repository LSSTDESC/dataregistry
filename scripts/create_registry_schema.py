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
from dataregistry.db_basic import _insert_provenance, _insert_keyword
from dataregistry.schema import load_schema, load_preset_keywords

"""
A script to create a schema.

The schema contains the following six tables:
    - "dataset"         : Primary table, contains information on the datasets
    - "dataset_alias"   : Table to associate "alias" names to datasets
    - "execution"       : Stores executions, datasets can be linked to these
    - "execution_alias" : Table to asscociate "alias" names to executions
    - "dependancy"      : Tracks dependencies between datasets
    - "provenance"      : Contains information about the database/schema
    - "keyword"         : A list of keywords that can be tagged to datasets
    - "dataset_keyword" : Many-many link between keywords and datasets
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
schema_data = load_schema()
schema_data = schema_data["tables"]


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
    for column in schema_data[table]["column_definitions"].keys():
        # Special case where column has a foreign key
        if schema_data[table]["column_definitions"][column]["foreign_key"]:
            fk_schema = schema
            if (
                schema_data[table]["column_definitions"][column]["foreign_key_schema"]
                != "self"
            ):
                fk_schema = schema_data[table]["column_definitions"][column][
                    "foreign_key_schema"
                ]

            return_dict[column] = Column(
                column,
                _TYPE_TRANSLATE[
                    schema_data[table]["column_definitions"][column]["type"]
                ],
                ForeignKey(
                    _get_ForeignKey_str(
                        fk_schema,
                        schema_data[table]["column_definitions"][column][
                            "foreign_key_table"
                        ],
                        schema_data[table]["column_definitions"][column][
                            "foreign_key_column"
                        ],
                    )
                ),
                primary_key=schema_data[table]["column_definitions"][column][
                    "primary_key"
                ],
                nullable=schema_data[table]["column_definitions"][column]["nullable"],
            )

        # Normal case
        else:
            return_dict[column] = Column(
                column,
                _TYPE_TRANSLATE[
                    schema_data[table]["column_definitions"][column]["type"]
                ],
                primary_key=schema_data[table]["column_definitions"][column][
                    "primary_key"
                ],
                nullable=schema_data[table]["column_definitions"][column]["nullable"],
            )

    return return_dict


def _get_table_metadata(schema, table):
    """
    Build the table meta data dict, e.g., the schema name and any unique
    constraints, for this table.

    Parameters
    ----------
    schema : str
    table : str

    Returns
    -------
    meta : dict
        The table metadata
    """

    # Table metadata
    meta = {
        "__tablename__": table,
    }

    table_args = []

    # Handle column indexes
    if "indexs" in schema_data[table].keys():
        for index_att in schema_data[table]["indexs"].keys():
            table_args.append(
                Index(*schema_data[table]["indexs"][index_att]["index_list"])
            )

    # Handle unique constraints
    if "unique_constraints" in schema_data[table].keys():
        for uq_att in schema_data[table]["unique_constraints"].keys():
            table_args.append(
                UniqueConstraint(
                    *schema_data[table]["unique_constraints"][uq_att]["unique_list"],
                    name=uq_att,
                    postgresql_nulls_not_distinct=True,
                )
            )

    # Bring it together
    if len(table_args) > 0:
        table_args.append({"schema": schema})
        meta["__table_args__"] = tuple(table_args)
    else:
        meta["__table_args__"] = {"schema": schema}

    return meta


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


def _FixDependencyColumns(columns, has_production, production):
    """
    Special case for dependencies table where some column names need to be tweeked.

    Columns dict is modified in place.

    Parameters
    ----------
    columns : dict
    has_production : bool
        True if database has a production schema
    production : str
        Name of the production schema
    """

    # Remove link to production schema if unneeded.
    if not has_production:
        del columns["input_production_id"]

    # Update production schema name
    else:
        if production != "production":
            old_col = columns["input_production_id"]
            fkey = ForeignKey(f"{production}.dataset.dataset_id")
            new_input_production_id = Column(old_col.name, old_col.type, fkey)
            del columns["input_production_id"]
            columns["input_production_id"] = new_input_production_id

def _BuildTable(schema, table_name, has_production, production):
    """
    Builds a generic schema table from the information in the `schema.yaml` file.

    Parameters
    ----------
    schema : str
    table_name : str
    has_production : bool
        True if database has a production schema
    production : str
        Name of the production schema

    Returns
    -------
    Model : class object
    """

    class_name = f"{schema}_{table_name}"

    # Column definitions (from `schema.yaml` file)
    columns = _get_column_definitions(schema, table_name)

    # Special case for dependencies table
    if table_name == "dependency":
        _FixDependencyColumns(columns, has_production, production)

    # Table metadata (from `schema.yaml` file)
    meta = _get_table_metadata(schema, table_name)

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model

def _Keyword(schema):
    """Stores the list of keywords."""

    class_name = f"{schema}_keyword"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "keyword")

    # Table metadata
    meta = {"__tablename__": "keyword", "__table_args__": (UniqueConstraint(
                "keyword", name="keyword_u_keyword"
            ), {"schema": schema},)}

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model

def _DatasetKeyword(schema):
    """Many-Many link between datasets and keywords."""

    class_name = f"{schema}_dataset_keyword"

    # Load columns from `schema.yaml` file
    columns = _get_column_definitions(schema, "dataset_keyword")

    # Table metadata
    meta = {"__tablename__": "dataset_keyword", "__table_args__": {"schema": schema}}

    Model = type(class_name, (Base,), {**columns, **meta})
    return Model

# The following should be adjusted whenever there is a change to the structure
# of the database tables.
_DB_VERSION_MAJOR = 3
_DB_VERSION_MINOR = 2
_DB_VERSION_PATCH = 0
_DB_VERSION_COMMENT = "Add replace columns to dataset table"

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
    "--production-schema",
    default="production",
    help="name of schema containing production tables.",
)
parser.add_argument("--config", help="Path to the data registry config file")

args = parser.parse_args()
schema = args.schema
prod_schema = args.production_schema

# Connect to database to find out what the backend is
db_connection = DbConnection(args.config, schema)
if db_connection.dialect == "sqlite":
    if schema == prod_schema:
        raise ValueError("Production not available for sqlite databases")
    # In fact we don't use schemas at all for sqlite
    schema = None
    prod_schema = None
else:
    if schema != prod_schema:
        # production schema, tables must already exists and schema
        # must be backwards-compatible with prod_schem.  That is, major
        # versions must match and minor version of prod_schema cannot
        # be greater than minor version of schema
        stmt = f"select db_version_major, db_version_minor from {prod_schema}.provenance order by provenance_id desc limit 1"
        try:
            with db_connection.engine.connect() as conn:
                result = conn.execute(text(stmt))
                result = pd.DataFrame(result)
                conn.commit()
        except Exception:
            raise RuntimeError("production schema does not exist or is ill-formed")
        if (
            result["db_version_major"][0]
            != _DB_VERSION_MAJOR | int(result["db_version_minor"][0])
            > _DB_VERSION_MINOR
        ):
            raise RuntimeError("production schema version incompatible")

if schema:
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

            if schema == prod_schema:  # also grant privileges to reg_writer
                acct = "reg_writer"
                usage_priv = f"GRANT USAGE ON SCHEMA {schema} to {acct}"
                select_priv = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} to {acct}"
                conn.execute(text(usage_priv))
                conn.execute(text(select_priv))
                conn.commit()
    except Exception as e:
        print(f"Could not grant access to {acct} on schema {schema}")

# Create the tables
for table_name in schema_data.keys():
    _BuildTable(schema, table_name, db_connection.dialect != "sqlite", prod_schema)

# Generate the database
if schema:
    if schema != prod_schema:
        Base.metadata.reflect(db_connection.engine, prod_schema)
Base.metadata.create_all(db_connection.engine)

# Grant access to other accounts.  Can only grant access to objects
# after they've been created
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
except Exception:
    print(f"Could not grant access to {acct} on schema {schema}")


# Add initial provenance information
db = DbConnection(args.config, schema)
prov_id = _insert_provenance(
    db,
    _DB_VERSION_MAJOR,
    _DB_VERSION_MINOR,
    _DB_VERSION_PATCH,
    "CREATE",
    comment=_DB_VERSION_COMMENT,
    associated_production=prod_schema,
)

# Populate the preset system keywords for datasets
keywords = load_preset_keywords()
for att in keywords["dataset"]:
    _insert_keyword(db, att, True)
