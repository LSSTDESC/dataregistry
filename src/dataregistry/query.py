import os
from datetime import datetime
from collections import namedtuple
from sqlalchemy import MetaData, Table, Column, text, select
import sqlalchemy.sql.sqltypes as sqltypes
try:
    import sqlalchemy.dialects.postgresql as pgtypes
    PG_TYPES = {pgtypes.TIMESTAMP, pgtypes.INTEGER, pgtypes.BIGINT,
                pgtypes.FLOAT, pgtypes.DOUBLE_PRECISION, pgtypes.NUMERIC, pgtypes.DATE}

except:
    PG_TYPES = {}
try:
    import sqlalchemy.dialects.sqlite as lite_types
    LITE_TYPES = {lite_types.DATE, lite_types.DATETIME, lite_types.FLOAT,
                  lite_types.INTEGER, lite_types.NUMERIC, lite_types.TIME,
                  lite_types.TIMESTAMP}
except:
    LITE_TYPES = {}

from sqlalchemy.exc import DBAPIError
from dataregistry.db_basic import add_table_row, SCHEMA_VERSION, ownertypeenum
from dataregistry.db_basic import TableMetadata
from dataregistry.registrar_util import form_dataset_path
from dataregistry.exceptions import *

__all__ = ['Query', 'Filter']

'''
Filters describe a restricted set of expressions which, ultimately,
may end up in an sql WHERE clause.
property_name must refer to a property belonging to datasets (column in dataset
or joinable table).
op may be one of '==', '!=', '<', '>', '<=', '>='. If the property in question is of
datatype string, only '==' or '!=' may be used.
value should be a constant (or expression?) of the same type as the property.
'''
Filter = namedtuple('Filter', ['property_name', 'bin_op', 'value'])

_colops = {'==' : '__eq__', '=' : '__eq__', '!=' : '__ne__',
           '<' : '__lt__', '<=' : '__le__',
           '>' : '__gt__', '>=' : '__ge__'}

ALL_ORDERABLE = {sqltypes.INTEGER, sqltypes.FLOAT, sqltypes.DOUBLE,
                 sqltypes.TIMESTAMP, sqltypes.DATETIME,
                 sqltypes.DOUBLE_PRECISION}.union(PG_TYPES).union(LITE_TYPES)

def is_orderable_type(ctype):
    return type(ctype) in ALL_ORDERABLE

class Query():
    '''
    Class implementing supported queries
    '''
    def __init__(self, db_engine, dialect, schema_version=SCHEMA_VERSION):
        self._engine = db_engine
        self._dialect = dialect
        if dialect == 'sqlite':
            self._schema_version = None
        else:
            self._schema_version=schema_version

        # Do we need to know where the datasets actually are?  If so
        # we need a ROOT_DIR

        self._metadata_getter = TableMetadata(self._schema_version,
                                              db_engine)

        # Get table definitions
        self._all_dataset_properties = None
        self._table_list = ["dataset", "execution", "dataset_alias"]
        self._get_database_tables()

    def _get_database_tables(self):
        '''
        Pulls out the table metadata from the data registry database and stores
        them in the self._tables dict.

        In addition, a dict is created for each table of the database which
        stores the column names of the table, and if those columns are of an
        orderable type. The dicts are named as self._<table_name>_columns. 

        This helps us with querying against those tables, and joining between
        them. 
        '''
        self._tables = dict()
        for table in self._table_list:
            # Metadata from table
            self._tables[table] = self._metadata_getter.get(table)

            # Pull out column names from table.
            setattr(self, f"_{table}_columns", dict())
            for c in self._tables[table].c:
                getattr(self, f"_{table}_columns")[table + "." + c.name] = is_orderable_type(c.type)

    def _parse_selected_columns(self, property_names):
        '''
        See what tables we need to work with (i.e., join) for a given property list.

        If the user is not explicit (i.e., uses <column_name> over
        <table_name>.<column_name>) the property names must be unique in the
        database, and not clash between tables.
        
        Parameters
        ----------
        property_names : list
            String list of database columns 
        '''

        tables_required = set()
        column_list = []
        is_orderable_list = []

        # Determine the column name and table for each property
        for p in property_names:

            # Case of <table_name>.<property_name> format
            if "." in p:
                assert len(p.split(".")) == 2, f"{p} is bad property name format"
                table_name = p.split(".")[0]
                col_name = p.split(".")[1]

            # Case of <property_name> only format
            else:
                col_name = p
                
                # Now find what table its from.
                found_count = 0
                for t in self._table_list:
                    if f"{t}.{col_name}" in getattr(self, f"_{t}_columns").keys():
                        found_count += 1
                        table_name = t

                # Was this name unique in the database?
                assert found_count > 0, f"Did not find any columns named {col_name}"
                assert found_count == 1, f"Column name '{col_name}' is not unique to one table in the database, use <table_name>.<column_name> format instead"

            tables_required.add(table_name)
            is_orderable_list.append(getattr(self, f"_{table_name}_columns")[table_name + "." + col_name])
            column_list.append(self._tables[table_name].c[col_name])

        return list(tables_required), column_list, is_orderable_list

    def _render_filter(self, f, stmt):
        '''
        Check that parts of the filter look ok.  Return statement with where clause
        appended
        '''

        _, column_ref, column_is_orderable = self._parse_selected_columns([f[0]])
        assert len(column_ref) == len(column_is_orderable) == 1

        # Extract the filter operator (also making sure it is an allowed one)
        if f[1] not in _colops.keys():
            raise ValueError(f'check_filter: "{f[1]}" is not a supported operator')
        else:
            the_op = _colops[f[1]]

        # Extract the property we are ordering on (also making sure is is orderable)
        if not column_is_orderable[0] and f[1] not in ['==', '=',  '!=']:
            raise ValueError('check_filter: Cannot apply "{f[1]}" to "{f[0]}"')
        else:
            value = f[2]

        return stmt.where(column_ref[0].__getattribute__(the_op)(value))

    def find_datasets(self, property_names=None, filters=[]):
        '''
        Get specified properties for datasets satisfying all filters.

        If property_names is None, return all properties from the dataset table
        (only). Otherwise, return the property_names columns for each
        discovered dataset (which can be from multiple tables via a join).

        Filters should be a list of Filter objects, which are constraints on
        column values.

        These choices get translated into an SQL query.

        Example queries
        ---------------
        find_datasets(property_names=None, filters=[])
            - SELECT * FROM registry_dev.dataset

        find_datasets(property_names=["dataset.name", "execution.name"], filters=[])
            - SELECT dataset.name, execution.name FROM registry_dev.dataset
              JOIN registry_dev.execution ON
              registry_dev.execution.execution_id =
              registry_dev.dataset.execution_id

        f = Filter("dataset.name", "==", "DESC dataset 1")
        find_datasets(property_names=["dataset.description"], filters=[f])
            - SELECT dataset.description FROM registry_dev.dataset WHERE
              registry_dev.dataset.name = :name_1

        Returns
        -------
        result : sqlAlchemy Result object
        '''

        # What tables are required for this query?
        tables_required, _, _ = self._parse_selected_columns(property_names)

        # Construct query

        # No properties requested, return all from dataset table (only)
        if property_names is None:
            stmt = select("*").select_from(self._tables['dataset'])

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
                print('Original error:')
                print(e.StatementError.orig)
                return None

        return result
