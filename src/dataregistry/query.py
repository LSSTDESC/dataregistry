import os
from datetime import datetime
from collections import namedtuple
from sqlalchemy import MetaData, Table, Column, text, select
from sqlalchemy.exc import DBAPIError
from dataregistry.db_basic import add_table_row, SCHEMA_VERSION, ownertypeenum
from dataregistry.registrar_util import form_dataset_path
from dataregistry.exceptions import *

__all__ = ['Query', 'Filter']

'''
Filters describe a restricted set of expressions which, ultimately,
may end up in an sql WHERE clause.
property_name must refer to a property belonging to datasets.
op may be one of '==', '<', '>', '<=', '>='. If the property in question is of
datatype string, only '==' may be used.
value should be a constant (or expression?) of the same type as the property.
'''
NamedTuple Filter = namedtuple('Filter', ['property_name', 'op', 'value'])

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

        self._metadata = MetaData(schema=self._schema_version)
        self._dataset_table = None
        self._execution_table = None
        self._dataset_alias_table = None
        self._execution_alias_table = None
        self._dependency_table = None

        # The initialization above is a subset of init for Register class.
        # Maybe should be a base class?

    def list_dataset_properties(self):
        '''
        Should return a list of property names (or maybe a list of duples
        (name, dtype)) not necessarily identical to column names in the
        dataset table, though possibly coming from a view.  For example,
        it should be possible to ask for alias names belonging to a
        dataset, or filter on alias name.
        '''
        pass

    def list_dataset_filter_properties(self):
        '''
        Should return a list of properties (probably duples (name, dtype))
        which may be used in filter expressions.
        Maybe filter_properties == properties, in which case we don't need
        this method.
        '''
        pass

    def list_execution_properties(self):
        '''
        Should return a list of property names, not necessarily identical
        to column names in the execution table, though possibly coming from
        a view. Or, maybe better, a list of duples (name, dtype)
        '''
        pass

    def find_datasets(self, property_names=None, filters=[]):
        '''
        Get specified properties for datasets satisfying all filters
        If property_names is None, return all properties.

        Return (probably) pandas dataframe
        '''
        pass
