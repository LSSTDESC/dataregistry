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
from dataregistry.registrar_util import form_dataset_path
from dataregistry.exceptions import *

__all__ = ['Query', 'Filter']

'''
Filters describe a restricted set of expressions which, ultimately,
may end up in an sql WHERE clause.
property_name must refer to a property belonging to datasets.
op may be one of '==', '!=', '<', '>', '<=', '>='. If the property in question is of
datatype string, only '==' or '!=' may be used.
value should be a constant (or expression?) of the same type as the property.
'''
Filter = namedtuple('Filter', ['property_name', 'bin_op', 'value'])

ALL_ORDERABLE = {sqltypes.INTEGER, sqltypes.FLOAT, sqltypes.DOUBLE,
                 sqltypes.TIMESTAMP, sqltypes.DATETIME,
                 sqltypes.DOUBLE_PRECISION}.union(PG_TYPES).union(LITE_TYPES)

def orderable(ctype):
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

        self._metadata = MetaData(schema=self._schema_version)

        # Get all table definitions
        self._metadata.reflect(db_engine)
        self._dataset_table = None
        self._execution_table = None
        # self._dataset_alias_table = None
        # self._execution_alias_table = None
        # self._dependency_table = None
        self._native = None
        self._from_execution = None

    def list_dataset_properties(self):
        '''
        Returns a dict where keys are column names (prefaced by table if not a
        native dataset column) and value is True if values are "orderable", else
        False.   Numeric types and date or datetime types are orderable. Others
        are not.
        '''
        # Get native quantities from metadata
        if self._dataset_table is None:
            # also loads execution table
            self._dataset_table = Table("dataset", self._metadata,
                                        autoload_with=self._engine)

        if self._native is None:
            self._native = dict()
            for c in self._metadata.tables[f'{self._schema_version}.dataset'].c:
                if c.name != 'execution_id':
                    self._native['dataset.' + c.name] = orderable(c.type)

        if self._from_execution is None:
            self._from_execution = dict()
            for c in self._metadata.tables[f'{self._schema_version}.execution'].c:
                self._from_execution['execution.' + c.name] = orderable(c.type)

        all_properties = dict(self._native)
        all_properties.update(self._from_execution)
        return all_properties

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

if __name__ == '__main__':
    from dataregistry.db_basic import create_db_engine
    import os
    config = os.path.join(os.getenv('HOME'), '.config_reg_reader')
    engine, dialect = create_db_engine(config)
    q = Query(engine, dialect, schema_version='registry_jrb')
    props = q.list_dataset_properties()
    print('All done')

    for k,v in props.items():
        print('key: ', k, '  value: ', v)
