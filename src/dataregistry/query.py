from collections import namedtuple
from sqlalchemy import select
import sqlalchemy.sql.sqltypes as sqltypes
import pandas as pd
from dataregistry.registrar.registrar_util import _form_dataset_path
from dataregistry.exceptions import DataRegistryNYI, DataRegistryException
from functools import reduce

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
    "~=": None,
    "~==": None,
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

ILIKE_ALLOWED = [
    "dataset.name",
    "dataset.owner",
    "dataset.relative_path",
    "dataset.access_api",
]


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
        self.db_connection = db_connection
        self.db_connection._reflect()

        self._engine = db_connection.engine
        self._dialect = db_connection.dialect
        self._schema = db_connection.schema
        self._root_dir = root_dir

    def get_all_columns(self, include_schema=False):
        """
        Return all columns of the db in <table_name>.<column_name> format.

        If `include_schema=True` return all columns of the db in
        <schema>.<table_name>.<column_name> format. Note this will essentially
        duplicate the output, as the working and production schemas have the
        same layout.

        Parameters
        ----------
        include_schema : bool, optional
            If True, also return the schema name in the column name

        Returns
        -------
        column_list : set
        """

        column_list = set()

        # Loop over each table
        for table in self.db_connection.metadata["tables"]:
            # Loop over each column
            for c in self.db_connection.metadata["tables"][table].c:
                if include_schema:
                    column_list.add(".".join((str(c.table), str(c.name))))
                else:
                    column_list.add(".".join((str(c.table.name), str(c.name))))

        return column_list

    def _parse_selected_columns(self, column_names):
        """
        What tables do we need for a given list of column names.

        Column names can be in <column_name>, <table_name>.<column_name> or If
        they are in <column_name> format the column name must be unique through
        all tables in the database.

        If column_names is None, all columns from the dataset table will be
        selected.

        Parameters
        ----------
        column_names : list
            String list of database columns

        Returns
        -------
        tables_required : list[str]
            All table names included in `column_names`
        column_list : dict[schema][list[sqlalchemy.sql.schema.Column]]
            All column objects for the columns included in `column_names`
        is_orderable_list : dict[schema][list[bool]]
            Is the column of an orderable type?
        """

        # Select all columns from the dataset table
        if column_names is None:
            column_names = []
            for table in self.db_connection.metadata["tables"]:
                tname = table if self.db_connection.dialect == "sqlite" else table.split(".")[1]
                if tname == "dataset":
                    column_names.extend(
                        [
                            x.table.name + "." + x.name
                            for x in self.db_connection.metadata["tables"][table].c
                        ]
                    )
                    break # Dont duplicate with production schema

        tables_required = set()
        column_list = {}
        is_orderable_list = {}

        # Loop over each input column
        for col_name in column_names:
            # Stores matches for this input
            tmp_column_list = {}

            # Split column name into its parts
            input_parts = col_name.split(".")
            num_parts = len(input_parts)

            if num_parts > 2:
                raise ValueError(f"{col_name} is not a valid column")

            # Loop over each column in the database and find matches
            for table in self.db_connection.metadata["tables"]:
                for column in self.db_connection.metadata["tables"][table].c:
                    X = str(column.table) + "." + column.name
                    table_parts = X.split(".")

                    if column.table.schema not in tmp_column_list.keys():
                        tmp_column_list[column.table.schema] = []

                    # Match based on the format of column_names
                    if num_parts == 1:
                        # Input is in <column> format
                        if input_parts[0] == table_parts[-1]:
                            tmp_column_list[column.table.schema].append(column)
                    elif num_parts == 2:
                        # Input is in <table>.<column> format
                        if (
                            input_parts[0] == table_parts[-2]
                            and input_parts[1] == table_parts[-1]
                        ):
                            tmp_column_list[column.table.schema].append(column)

            # Make sure we don't find multiple matches
            for s in tmp_column_list.keys(): # Each schema
                chk = []
                for x in tmp_column_list[s]: # Each column in schema
                    if x.name in chk:
                        raise DataRegistryException(
                            (
                                f"Column name '{col_name}' is not unique to one table "
                                f"in the database, use <table_name>.<column_name> "
                                f"format instead"
                            )
                        )
                    chk.append(x.name)

                    # Add this table to the list
                    tables_required.add(x.table.name)

            # Store results
            for att in tmp_column_list.keys():
                if att not in column_list.keys():
                    column_list[att] = []
                column_list[att].extend(tmp_column_list[att])

                if att not in is_orderable_list.keys():
                    is_orderable_list[att] = []
                is_orderable_list[att].extend(
                    [is_orderable_type(c.type) for c in tmp_column_list[att]]
                )

        return list(tables_required), column_list, is_orderable_list

    def _render_filter(self, f, stmt, schema):
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
            Updated query appended with additional SQL WHERE clause
        """

        # Get the reference to the column being filtered on.
        _, column_ref, column_is_orderable = self._parse_selected_columns([f[0]])

        # Extract the filter operator (also making sure it is an allowed one)
        if f[1] not in _colops.keys():
            raise ValueError(f'check_filter: "{f[1]}" is not a supported operator')
        else:
            the_op = _colops[f[1]]

        # Extract the property we are ordering on (also making sure it
        # is orderable)
        if not column_is_orderable[schema][0] and f[1] not in [
            "~==",
            "~=",
            "==",
            "=",
            "!=",
        ]:
            raise ValueError('check_filter: Cannot apply "{f[1]}" to "{f[0]}"')
        else:
            value = f[2]

        # String partial matching with wildcard
        if f[1] in ["~=", "~=="]:
            if f[0] not in ILIKE_ALLOWED:
                raise ValueError(f"Can only perform ~= search on {ILIKE_ALLOWED}")

            tmp = value.replace("%", r"\%").replace("_", r"\_").replace("*", "%")

            # Case insensitive wildcard matching (wildcard is '*')
            if f[1] == "~=":
                return stmt.where(column_ref[schema][0].ilike(tmp))
            # Case sensitive wildcard matching (wildcard is '*')
            else:
                return stmt.where(column_ref[schema][0].like(tmp))

        # General case using traditional boolean operator
        else:
            return stmt.where(column_ref[schema][0].__getattribute__(the_op)(value))

    def _append_filter_tables(self, tables_required, filters):
        """
        A list of tables required to join is initially built from the return
        columns in `property_names`. However there may be additional tables in
        the filters that are not part of the return columns, add them here.

        Parameters
        ----------
        tables_required : list
            Current list of tables from `property_names`
        filters : list
            The list of filters

        Returns
        -------
        tables_required : list
            Updated list of tables required now also considering filters
        """

        tables_required = set(tables_required)

        # Loop over each filter and add the tables to the list
        for f in filters:
            tmp_tables_required, _, _ = self._parse_selected_columns([f[0]])

            for t in tmp_tables_required:
                tables_required.add(t)

        return list(tables_required)

    def get_db_versioning(self):
        """
        returns
        -------
        major, minor, patch      int   version numbers for db OR
        None, None, None     in case db is too old to contain provenance table
        """
        raise NotImplementedError()
        #return (
        #    self._metadata.db_version_major,
        #    self._metadata.db_version_minor,
        #    self._metadata.db_version_patch,
        #)

    def find_datasets(
        self,
        property_names=None,
        filters=[],
        verbose=False,
        return_format="property_dict",
        strip_table_names=False,
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
            "DataFrame", or "proprety_dict". Note this is not case sensitive.
        strip_table_names : bool, optional
            True to remove the table name in the results columns
            This only works if a single table is needed for the query 

        Returns
        -------
        result : dict, or DataFrame (depending on `return_format`)
            Requested property values
        """

        # Make sure return format is valid.
        _allowed_return_formats = ["dataframe", "property_dict"]
        if return_format.lower() not in _allowed_return_formats:
            raise ValueError(
                f"{return_format} is a bad return format (valid={_allowed_return_formats})"
            )

        results = []

        # What tables and what columns are required for this query?
        tables_required, column_list, _ = self._parse_selected_columns(property_names)
        tables_required = self._append_filter_tables(tables_required, filters)

        # Can only strip table names for queries against a single table
        if strip_table_names and len(tables_required) > 1:
            raise DataRegistryException(
                    "Can only strip out table names "
                    "for single table queries"
                )

        # Construct query
        for schema in column_list.keys(): # Loop over each schema
            schema_str = "" if self.db_connection.dialect == "sqlite" else f"{schema}."
            columns = [f"{p.table.name}.{p.name}" for p in column_list[schema]]

            stmt = select(
                *[p.label(f"{p.table.name}.{p.name}") for p in column_list[schema]]
            )
            
            # Create joins
            if len(tables_required) > 1:
                j = self.db_connection.metadata["tables"][f"{schema_str}dataset"]
                for i in range(len(tables_required)):
                    if tables_required[i] in ["dataset", "keyword", "dependency"]:
                        continue

                    j = j.join(
                        self.db_connection.metadata["tables"][
                            f"{schema_str}{tables_required[i]}"
                        ]
                    )

                # Special case for many-to-many keyword join
                if "keyword" in tables_required:
                    j = j.join(
                        self.db_connection.metadata["tables"][
                            f"{schema_str}dataset_keyword"
                        ]
                    ).join(self.db_connection.metadata["tables"][f"{schema_str}keyword"])

                # Special case for dependencies
                if "dependency" in tables_required:
                    dataset_table = self.db_connection.metadata["tables"][f"{schema_str}dataset"]
                    dependency_table = self.db_connection.metadata["tables"][f"{schema_str}dependency"]
                    
                    j = j.join(
                        dependency_table,
                        dependency_table.c.input_id == dataset_table.c.dataset_id  # Explicit join condition
                    )

                stmt = stmt.select_from(j)
            else:
                stmt = stmt.select_from(
                    self.db_connection.metadata["tables"][
                        f"{schema_str}{tables_required[0]}"
                    ]
                )

            # Append filters if acceptable
            if len(filters) > 0:
                for f in filters:
                    stmt = self._render_filter(f, stmt, schema)

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

            # Store result
            results.append(pd.DataFrame(result))

        # Combine results across schemas
        return_result = pd.concat(results, ignore_index=True)

        # Strip out table name from the headers
        if strip_table_names:
            return_result.rename(columns=lambda x: x.split('.')[-1], inplace=True)

        if return_format.lower() == "property_dict":
            return return_result.to_dict("list")
        else:
            return return_result

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
        Find what an alias points to.  May be either a dataset or another
        alias (or nothing)

        Parameters
        ----------
        alias      String or int      Either name or id of an alias

        Returns
        -------
        id         int                id of item (dataset or alias)
                                      referred to
        ref_type   string             type of object aliased to,
                                      either "dataset" or "alias"

        If no such alias is found, return None, None
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
                        raise DataRegistryException(f"find_aliases: no such column {p}")
                else:
                    raise DataRegistryException(f"find_aliases: no such column {p}")
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
