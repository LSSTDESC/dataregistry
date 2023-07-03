import os
from datetime import datetime
from collections import namedtuple
from sqlalchemy import MetaData, Table, Column, text, select
import sqlalchemy.sql.sqltypes as sqltypes

try:
    import sqlalchemy.dialects.postgresql as pgtypes

    PG_TYPES = {
        pgtypes.TIMESTAMP,
        pgtypes.INTEGER,
        pgtypes.BIGINT,
        pgtypes.FLOAT,
        pgtypes.DOUBLE_PRECISION,
        pgtypes.NUMERIC,
        pgtypes.DATE,
    }

except:
    PG_TYPES = {}
try:
    import sqlalchemy.dialects.sqlite as lite_types

    LITE_TYPES = {
        lite_types.DATE,
        lite_types.DATETIME,
        lite_types.FLOAT,
        lite_types.INTEGER,
        lite_types.NUMERIC,
        lite_types.TIME,
        lite_types.TIMESTAMP,
    }
except:
    LITE_TYPES = {}

from sqlalchemy.exc import DBAPIError, NoSuchColumnError
from dataregistry.db_basic import add_table_row, SCHEMA_VERSION, ownertypeenum
from dataregistry.db_basic import TableMetadata
from dataregistry.exceptions import *

__all__ = ["Query", "Filter"]

"""
Filters describe a restricted set of expressions which, ultimately,
may end up in an sql WHERE clause.
property_name must refer to a property belonging to datasets (column in dataset
or joinable table).
op may be one of '==', '!=', '<', '>', '<=', '>='. If the property in question is of
datatype string, only '==' or '!=' may be used.
value should be a constant (or expression?) of the same type as the property.
"""
Filter = namedtuple("Filter", ["property_name", "bin_op", "value"])

_colops = {
    "==": "__eq__",
    "=": "__eq__",
    "!=": "__ne__",
    "<": "__lt__",
    "<=": "__le__",
    ">": "__gt__",
    ">=": "__ge__",
}

ALL_ORDERABLE = (
    {
        sqltypes.INTEGER,
        sqltypes.FLOAT,
        sqltypes.DOUBLE,
        sqltypes.TIMESTAMP,
        sqltypes.DATETIME,
        sqltypes.DOUBLE_PRECISION,
    }
    .union(PG_TYPES)
    .union(LITE_TYPES)
)


def is_orderable_type(ctype):
    return type(ctype) in ALL_ORDERABLE


class Query:
    """
    Class implementing supported queries
    """

    def __init__(self, db_engine, dialect, schema_version=SCHEMA_VERSION):
        '''
        Create a new Query object. Note this call should be preceded
        by a call to create_db_engine, which will return values for
        db_engine and dialect

        Parameters
        ----------
        db_engine :   sqlalchemy engine object
        dialect : str
            identifies target db type (e.g. 'postgresql')
        schema_version : str
            Which database schema to connect to.
            Current default is 'registry_beta'
        '''
        self._engine = db_engine
        self._dialect = dialect
        if dialect == "sqlite":
            self._schema_version = None
        else:
            self._schema_version = schema_version

        # Do we need to know where the datasets actually are?  If so
        # we need a ROOT_DIR

        self._metadata = TableMetadata(self._schema_version, db_engine)

        # Get table definitions
        self._table_list = ["dataset", "execution", "dataset_alias", "dependency"]
        self._get_database_tables()

    def get_all_columns(self):
        """
        Return all columns of the database in <table_name>.<column_name> format.

        Returns
        -------
        column_list : list
        """

        column_list = []
        for table in self._table_list:
            for att in getattr(self, f"_{table}_columns"):
                column_list.append(att)

        return column_list

    def _get_database_tables(self):
        """
        Pulls out the table metadata from the data registry database and stores
        them in the self._tables dict.

        In addition, a dict is created for each table of the database which
        stores the column names of the table, and if those columns are of an
        orderable type. The dicts are named as self._<table_name>_columns.

        This helps us with querying against those tables, and joining between
        them.
        """
        self._tables = dict()
        for table in self._table_list:
            # Metadata from table
            self._tables[table] = self._metadata.get(table)

            # Pull out column names from table.
            setattr(self, f"_{table}_columns", dict())
            for c in self._tables[table].c:
                getattr(self, f"_{table}_columns")[
                    table + "." + c.name
                ] = is_orderable_type(c.type)

    def _parse_selected_columns(self, column_names):
        """
        What tables do we need for a given list of column names.

        Column names can be in <column_name> or <table_name>.<column_name>
        format. If they are in <column_name> format the column name must be
        unique through all tables in the database.

        Parameters
        ----------
        column_names : list
            String list of database columns
        """

        tables_required = set()
        column_list = []
        is_orderable_list = []

        # Determine the column name and table it comes from
        for p in column_names:

            # Case of <table_name>.<column_name> format
            if "." in p:
                if len(p.split(".")) != 2:
                    raise ValueError(f"{p} is bad column name format")
                table_name = p.split(".")[0]
                col_name = p.split(".")[1]

            # Case of <column_name> only format
            else:
                col_name = p

                # Now find what table its from.
                found_count = 0
                for t in self._table_list:
                    if f"{t}.{col_name}" in getattr(self, f"_{t}_columns").keys():
                        found_count += 1
                        table_name = t

                # Was this column name found, and is it unique in the database?
                if found_count == 0:
                    raise NoSuchColumnError(
                        f"Did not find any columns named {col_name}"
                    )
                elif found_count > 1:
                    raise DataRegistryException(
                        (
                            f"Column name '{col_name}' is not unique to one table"
                            f"in the database, use <table_name>.<column_name>"
                            f"format instead"
                        )
                    )

            tables_required.add(table_name)
            is_orderable_list.append(
                getattr(self, f"_{table_name}_columns")[table_name + "." + col_name]
            )
            column_list.append(self._tables[table_name].c[col_name])

        return list(tables_required), column_list, is_orderable_list

    def _render_filter(self, f, stmt):
        """
        Append SQL statement with an additional WHERE clause based on a
        dataregistry filter.

        Parameters
        ----------
        f : dataregistry filter
            Logic filter to be appended to SQL query
        stmt : sql alchemy Query object
            Current SQL query

        Returns
        -------
        - : sql alchemy Query object
            Updated query with new WHERE clause
        """

        # Get the reference to the column being filtered on.
        _, column_ref, column_is_orderable = self._parse_selected_columns([f[0]])
        assert len(column_ref) == len(column_is_orderable) == 1

        # Extract the filter operator (also making sure it is an allowed one)
        if f[1] not in _colops.keys():
            raise ValueError(f'check_filter: "{f[1]}" is not a supported operator')
        else:
            the_op = _colops[f[1]]

        # Extract the property we are ordering on (also making sure is is orderable)
        if not column_is_orderable[0] and f[1] not in ["==", "=", "!="]:
            raise ValueError('check_filter: Cannot apply "{f[1]}" to "{f[0]}"')
        else:
            value = f[2]

        return stmt.where(column_ref[0].__getattribute__(the_op)(value))

    def get_db_versioning(self):
        """
        returns
        -------
        major, minor, patch      int   version numbers for db OR
        None, None, None     in case db is too old to contain provenance table
        """
        return self._metadata.db_version_major, self._metadata.db_version_minor, self._metadata.db_version_patch

    def find_datasets(self, property_names=None, filters=[]):
        """
        Get specified properties for datasets satisfying all filters

        If property_names is None, return all properties from the dataset table
        (only). Otherwise, return the property_names columns for each
        discovered dataset (which can be from multiple tables via a join).

        Filters should be a list of dataregistry Filter objects, which are
        logic constraints on column values.

        These choices get translated into an SQL query.

        Parameters
        ----------
        property_names : list (optional)
            List of database columns to return (SELECT clause)
        filters : list (optional)
            List of filters (WHERE clauses) to apply

        Returns
        -------
        result : sqlAlchemy Result object
        """

        # What tables are required for this query?
        tables_required, _, _ = self._parse_selected_columns(property_names)

        # Construct query

        # No properties requested, return all from dataset table (only)
        if property_names is None:
            stmt = select("*").select_from(self._tables["dataset"])

        # Return the selected properties.
        else:
            stmt = select(*[text(p) for p in property_names])

            # Create joins
            if len(tables_required) > 1:
                j = self._tables["dataset"]
                for i in range(len(tables_required)):
                    if tables_required[i] == "dataset":
                        continue

                    j = j.join(self._tables[tables_required[i]])

                stmt = stmt.select_from(j)
            else:
                stmt = stmt.select_from(self._tables[tables_required[0]])

        # Append filters if acceptable
        if len(filters) > 0:
            for f in filters:
                stmt = self._render_filter(f, stmt)

        # Execute the query
        with self._engine.connect() as conn:
            try:
                result = conn.execute(stmt)
                conn.commit()
            except DBAPIError as e:
                print("Original error:")
                print(e.StatementError.orig)
                return None

        return result
