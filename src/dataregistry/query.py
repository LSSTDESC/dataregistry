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
        self._tables = dict()
        self._tables["dataset"] = self._metadata_getter.get("dataset")
        self._tables["execution"] = self._metadata_getter.get("execution")
        # self._dataset_alias_table = None
        # self._execution_alias_table = None
        # self._dependency_table = None
        self._dataset_native = None
        self._from_execution = None
        self._all_dataset_properties = None
        self._get_properties()

    def _get_properties(self):
        '''
        Cache column names which may be queried.
        '''
        if self._dataset_native is None:
            self._dataset_native = dict()
            for c in self._tables['dataset'].c:
                self._dataset_native['dataset.' + c.name] = is_orderable_type(c.type)

        if  self._from_execution is None:
            self._from_execution = dict()
            for c in self._tables['execution'].c:
                self._from_execution['execution.' + c.name] = is_orderable_type(c.type)

    def list_dataset_properties(self):
        '''
        Returns a set of property names of the form <table_name>.<column_name>
        <table_name> is either 'dataset' or another table name (e.g. 'execution')
        for a table which may be joined with dataset.
        '''
        if self._all_dataset_properties:
            return self._all_dataset_properties.keys()

        all_properties = dict(self._dataset_native)
        all_properties.update(self._from_execution)
        self._all_dataset_properties = dict(all_properties)
        return list(all_properties.keys())

    def is_orderable_property(self, property_name):
        if not self._all_dataset_properties:
            self.list_dataset_properties()
        if not property_name in self._all_dataset_properties.keys():
            raise ValueException(f'Unknown property {property_name}')
        return self._all_dataset_properties[property_name]

    # Do we need this routine?  Will users want to query executions?
    def list_execution_properties(self):
        '''
        Should return a list of property names, not necessarily identical
        to column names in the execution table.
        '''
        raise NotImplementedError('Query.list_execution_properties is not implemented')

    def _check_property_name(self, c):
        '''
        Given string representation of form <column_name> or
        <table_name>.<column_name> check that it identifies a column belonging to the
        dataset table or a table it references or is associated with
        If input is just <column_name> and that name appears in more than one relevant
        table, raise ValueException

        Parameters
        c      string   identifies possible column
        Returns
        table name, column name, column reference
        '''
        parts = c.split('.')
        if len(parts) > 2:
            raise ValueException(f'check_filter: "{c}" is not of proper form <table_name>.<column_name> for a dataset property: too many "."')
        col = parts[-1]
        if len(parts) == 2:
            if c not in self._all_dataset_properties:
                raise ValueException(f'check_filter: "{c}" not found among dataset properties')
            else:
                tblname = parts[0]
        else:     # search for table
            in_tbl = []
            for tbl in self._tables:
                if col in tbl.columns:
                    in_tbl.append(tbl)
            if len(in_tbl) == 0:
                raise ValueException(f'Column {col} not found')
            if len(in_tbl) > 1:
                raise ValueException(f'Column {col} appears in more than one table. Include table in specification: <table>.{col}')
            tblname = in_tbl[0]

        return tblname, col, self._tables[tblname].c[col]

    def _render_filter(self, f, stmt):
        '''
        Check that parts of the filter look ok.  Return statement with where clause
        appended
        '''
        tbl_name, column_name, column_ref = self._check_property_name(f[0])

        if f[1] not in _colops.keys():
            raise ValueException(f'check_filter: "{f[1]}" is not a supported operator')
        else:
            the_op = _colops[f[1]]
        if not self.is_orderable_property(f[0]) and f[1] not in ['==', '=',  '!=']:
            raise ValueException('check_filter: Cannot apply "{f[1]}" to "{f[0]}"')
        else:
            value = f[2]

        return stmt.where(column_ref.__getattribute__(the_op)(value))

    def find_datasets(self, property_names=None, filters=[]):
        '''
        Get specified properties for datasets satisfying all filters

        If property_names is None, return all properties. Otherwise it should be
        a sublist of the return from list_dataset_properties

        filters should be a list of Filter objects, which are constraints on
        column values

        Return (probably) pandas dataframe
        '''
        if not self._all_dataset_properties:
            self.list_dataset_properties()

        # Determine if we need a join or not
        j = self._tables['dataset'].join(self._tables['execution'])
        if not property_names:
            # join dataset and execution tables
            stmt = select('dataset_table').select_from(j)
        else:
            stmt = select(*[text(p) for p in property_names])
            tbls = set()
            for p in property_names:
                tbls.update({p.split('.')[0]})
            if len(tbls) == 2:
                stmt = stmt.select_from(j)
            else:
                stmt = stmt.select_from(self._tables['dataset'])

        # Append filters if acceptable
        if len(filters) > 0:
            for f in filters:
                #f = self._render_filter(f)
                #stmt = stmt.where(f)
                stmt = self._render_filter(f, stmt)
        with self._engine.connect() as conn:
            try:
                result = conn.execute(stmt)
                conn.commit()
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None

        return result

if __name__ == '__main__':
    from dataregistry.db_basic import create_db_engine
    import os
    config = os.path.join(os.getenv('HOME'), '.config_reg_reader')
    engine, dialect = create_db_engine(config)
    q = Query(engine, dialect, schema_version='registry_jrb')
    props = q.list_dataset_properties()

    name_filter = Filter('dataset.name', '==', 'old.bashrc')
    minor_filter = Filter('dataset.version_minor', '<', 2)

    results = q.find_datasets(['dataset.dataset_id', 'dataset.name',
                               'dataset.version_minor', 'dataset.version_patch'], [name_filter])

    print('Name filter only:')
    for r in results:
        print(f'dataset_id: {r[0]} name: {r[1]} version_minor: {r[2]} version_patch: {r[3]}')

    results = q.find_datasets(['dataset.dataset_id', 'dataset.name',
                               'dataset.version_minor', 'dataset.version_patch'],
                              [name_filter, minor_filter])

    print('\nName filter and version_minor filter:')
    for r in results:
        print(f'dataset_id: {r[0]} name: {r[1]} version_minor: {r[2]} version_patch: {r[3]}')
