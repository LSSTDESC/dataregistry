from collections import namedtuple
from sqlalchemy import select
import sqlalchemy.sql.sqltypes as sqltypes
import pandas as pd
from dataregistry.registrar.registrar_util import _form_dataset_path
from dataregistry.exceptions import DataRegistryNYI, DataRegistryException

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

except ModuleNotFoundError:
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
except ModuleNotFoundError:
    LITE_TYPES = {}

from sqlalchemy.exc import DBAPIError, NoSuchColumnError
from dataregistry.db_basic import TableMetadata

__all__ = ["Query", "Filter"]

"""
Filters describe a restricted set of expressions which, ultimately,
may end up in an sql WHERE clause.
property_name must refer to a property belonging to datasets (column in
dataset or joinable table).
op may be one of '==', '!=', '<', '>', '<=', '>='. If the property in question
is of datatype string, only '==' or '!=' may be used.
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

    def __init__(self, db_connection, root_dir):
        """
        Create a new Query object. Note this call should be preceded
        by creation of a DbConnection object

        Parameters
        ----------
        db_connection : DbConnection object
            Encompasses sqlalchemy engine, dialect (database backend)
            and schema version
        root_dir : str
            Used to form absolute path of dataset
        """
        self._engine = db_connection.engine
        self._dialect = db_connection.dialect
        self._schema = db_connection.schema
        self._root_dir = root_dir

        self._metadata = TableMetadata(db_connection)

        # Get table definitions
        self._table_list = [
            "dataset",
            "execution",
            "dataset_alias",
            "dependency",
            "keyword",
            "dataset_keyword",
        ]
        self._get_database_tables()

    def get_all_columns(self):
        """
        Return all columns of the db in <table_name>.<column_name> format.

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

        If column_names is None, all columns from the dataset table will be
        selected.

        Parameters
        ----------
        column_names : list
            String list of database columns
        """

        if column_names is None:
            column_names = [
                x.table.name + "." + x.name for x in self._tables["dataset"].c
            ]

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

        # Extract the property we are ordering on (also making sure it
        # is orderable)
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
        return (
            self._metadata.db_version_major,
            self._metadata.db_version_minor,
            self._metadata.db_version_patch,
        )

    def find_datasets(
        self,
        property_names=None,
        filters=[],
        verbose=False,
        return_format="property_dict",
    ):
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
        property_names : list, optional
            List of database columns to return (SELECT clause)
        filters : list, optional
            List of filters (WHERE clauses) to apply
        verbose : bool, optional
            True for more output relating to the query
        return_format : str, optional
            The format the query result is returned in.  Options are
            "CursorResult" (SQLAlchemy default format), "DataFrame", or
            "proprety_dict". Note this is not case sensitive.

        Returns
        -------
        result : CursorResult, dict, or DataFrame (depending on `return_format`)
            Requested property values
        """

        # Make sure return format is valid.
        _allowed_return_formats = ["cursorresult", "dataframe", "property_dict"]
        if return_format.lower() not in _allowed_return_formats:
            raise ValueError(
                f"{return_format} is a bad return format (valid={_allowed_return_formats})"
            )

        # What tables and what columns are required for this query?
        tables_required, column_list, _ = self._parse_selected_columns(property_names)

        # Construct query

        # No properties requested, return all from dataset table (only)
        if property_names is None:
            stmt = select("*").select_from(self._tables["dataset"])

        # Return the selected properties.
        else:
            stmt = select(*[p.label(p.table.name + "." + p.name) for p in column_list])

            # Create joins
            if len(tables_required) > 1:
                j = self._tables["dataset"]
                for i in range(len(tables_required)):
                    if tables_required[i] in ["dataset", "keyword"]:
                        continue

                    j = j.join(self._tables[tables_required[i]])

                # Special case for many-to-many keyword join
                if "keyword" in tables_required:
                    j = j.join(self._tables["dataset_keyword"]).join(
                        self._tables["keyword"]
                    )

                stmt = stmt.select_from(j)
            else:
                stmt = stmt.select_from(self._tables[tables_required[0]])

        # Append filters if acceptable
        if len(filters) > 0:
            for f in filters:
                stmt = self._render_filter(f, stmt)

        # Report the constructed SQL query
        if verbose:
            print(f"Executing query: {stmt}")

        # Execute the query
        with self._engine.connect() as conn:
            try:
                result = conn.execute(stmt)
            except DBAPIError as e:
                print("Original error:")
                print(e.StatementError.orig)
                return None

        # Make sure we are working with the correct return format.
        if return_format.lower() != "cursorresult":
            result = pd.DataFrame(result)

            if return_format.lower() == "property_dict":
                result = result.to_dict("list")

        return result

    def gen_filter(self, property_name, bin_op, value):
        """
        Generate a binary filter for a data registry query.

        These construct SQL WHERE clauses.

        Parameters
        ----------
        property_name : str
            Database property to be queried on
        bin_op : str
            Binary operation to perform, e.g., "==" or ">="
        value : -
            Comparison value

        Returns
        -------
        - : namedtuple
            The Filter tuple

        Example
        -------
        .. code-block:: python

           f = datareg.Query.gen_filter("dataset.name", "==", "my_dataset")
           f = datareg.Query.gen_filter("dataset.version_major", ">", 1)
        """

        return Filter(property_name, bin_op, value)

    def get_dataset_absolute_path(self, dataset_id, schema=None):
        """
        Return full absolute path of specified dataset

        Parameters
        ----------
        dataset_id : int
            Identifies dataset
        schema : str
            Defaults to current schema, but may also be "production"

        Returns
        -------
        abs_path : str
        """

        if not schema:
            schema = self._schema

        # Fetch absolute path, owner type and owner for the dataset
        if schema != self._schema:
            raise DataRegistryNYI("schema != default is not yet supported")

        results = self.find_datasets(
            property_names=[
                "dataset.owner_type",
                "dataset.owner",
                "dataset.relative_path",
            ],
            filters=[("dataset.dataset_id", "==", dataset_id)],
        )
        if len(results["dataset.owner_type"]) == 1:
            return _form_dataset_path(
                results["dataset.owner_type"][0],
                results["dataset.owner"][0],
                results["dataset.relative_path"][0],
                schema=schema,
                root_dir=self._root_dir,
            )
        else:
            print(f"No dataset with dataset_id={dataset_id}")
            return None

    def resolve_alias(self, alias):
        """
        Parameters
        ----------
        alias      String or int      Either name or id of an alias

        Returns
        -------
        id         int                id of item (dataset or alias)
                                      referred to
        ref_type   string             type of object aliased to,
                                      either "dataset" or "alias"
        """
        tbl = self._tables["dataset_alias"]
        if isinstance(alias, int):
            filter_column = "dataset_alias.dataset_alias_id"
        elif isinstance(alias, str):
            filter_column = "dataset_alias.alias"
        else:
            raise ValueError("Argument 'alias' must be int or str")
        f = Filter(filter_column, "==", alias)

        stmt = select(tbl.c.dataset_id, tbl.c.ref_alias_id)
        stmt = stmt.select_from(tbl)
        stmt = self._render_filter(f, stmt)

        with self._engine.connect() as conn:
            try:
                result = conn.execute(stmt)
            except DBAPIError as e:
                print("Original error:")
                print(e.StatementError.orig)
                return None

        row = result.fetchone()
        if not row:
            # Print warning?
            return None, None
        if row[0]:
            return row[0], "dataset"
        else:
            return row[1], "alias"

    def resolve_alias_fully(self, alias):
        """
          Given alias id or name, return id of dataset it ultimately
          references
        """
        id, id_type = self.resolve_alias(alias)
        while id_type == "alias":
            id, id_type = self.resolve_alias(id)

        return id

    def find_aliases(
        self,
        property_names=None,
        filters=[],
        verbose=False,
        return_format="property_dict",
    ):

        """
        Return requested columns from dataset_alias table, subject to filters

        Parameters
        ----------
        property_names : list(str), optional
            List of database columns to return (SELECT clause)
        filters : list(Filter), optional
            List of filters (WHERE clauses) to apply
        verbose : bool, optional
            True for more output relating to the query
        return_format : str, optional
            The format the query result is returned in.  Options are
            "CursorResult" (SQLAlchemy default format), "DataFrame", or
            "proprety_dict". Note this is not case sensitive.
        """

        # Make sure return format is valid.
        _allowed_return_formats = ["cursorresult", "dataframe", "property_dict"]
        if return_format.lower() not in _allowed_return_formats:
            raise ValueError(
                f"{return_format} is a bad return format (valid={_allowed_return_formats})"
            )

        # This is always a query of a single table: dataset_alias
        tbl_name = "dataset_alias"
        tbl = self._tables[tbl_name]
        if property_names is None:
            stmt = select("*").select_from(tbl)

        else:
            cols = []
            for p in property_names:
                cmps = p.split(".")
                if len(cmps) == 1:
                    cols.append(tbl.c[p])
                elif len(cmps) == 2:
                    if cmps[0] == "dataset_alias":  # all is well
                        cols.append(tbl.c[cmps[1]])
                    else:
                        raise DataRegistryException(
                            f"find_aliases: no such column {p}"
                        )
                else:
                    raise DataRegistryException(
                        f"find_aliases: no such column {p}"
                    )
            stmt = select(*[p.label("dataset_alias." + p.name) for p in cols])
        # Append filters if acceptable
        if len(filters) > 0:
            for f in filters:
                stmt = self._render_filter(f, stmt)

        # Report the constructed SQL query
        if verbose:
            print(f"Executing query: {stmt}")

        # Execute the query
        with self._engine.connect() as conn:
            try:
                result = conn.execute(stmt)
            except DBAPIError as e:
                print("Original error:")
                print(e.StatementError.orig)
                return None

        # Make sure we are working with the correct return format.
        if return_format.lower() != "cursorresult":
            result = pd.DataFrame(result)

            if return_format.lower() == "property_dict":
                result = result.to_dict("list")

        return result
