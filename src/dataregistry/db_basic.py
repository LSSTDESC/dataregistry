from sqlalchemy import engine_from_config
from sqlalchemy.engine import make_url
import enum
from sqlalchemy import MetaData, Table, Enum, Column
from sqlalchemy import  column, text, insert, select
from sqlalchemy.exc import DBAPIError, IntegrityError
import yaml
import os
from collections import namedtuple

'''
Low-level utility routines and classes for accessing the registry
'''
SCHEMA_VERSION = 'registry_dev'

__all__ = ['create_db_engine', 'add_table_row', 'TableCreator',
           'TableMetadata', 'SCHEMA_VERSION', 'ownertypeenum', 'dataorgenum']

def create_db_engine(config_file):
    # Ideally config_file does not contain password, but if it does
    # it should be accessible to owner only
    with open(config_file) as f:
        connection_parameters = yaml.safe_load(f)
        driver = make_url(connection_parameters['sqlalchemy.url']).drivername
        dialect = driver.split('+')[0]

        return engine_from_config(connection_parameters), dialect

class ownertypeenum(enum.Enum):
    production = "production"
    group = "group"
    user = "user"

class dataorgenum(enum.Enum):
    file = "file"
    directory = "directory"
    dummy = "dummy"

def add_table_row(conn, table_meta, values, commit=True):
    '''
    Generic insert, given connection, metadata for a table and
    column values to be used.
    Return primary key for new row if successful
    '''
    result = conn.execute(insert(table_meta), [values])
    if commit:
        conn.commit()
    return result.inserted_primary_key[0]

class TableCreator:
    def __init__(self, engine, dialect, schema=SCHEMA_VERSION):
        self._engine = engine
        self._schema = schema
        self._dialect = dialect
        self._metadata = MetaData(schema=schema)

    def define_table(self, name, columns, constraints=[]):
        '''
        Usual case: caller wants to create a collection of tables all at once
        so just stash definition in MetaData object.

        Parameters
        name         string          table name
        columns      list of sqlalchemy.Column objects
        constraints  (optional) list of sqlalchemy.Constraint objects

        returns     tbl, an sqlalchemy.Table object.
                    User may instantiate immediately with
                    sqlalchemy.Table.create  method

        '''
        tbl = Table(name, self._metadata, *columns)
        for c in constraints:
            tbl.append_constraint(c)

        return tbl

    def create_table(self, name, columns, constraints=None):
        ''' Define and instantiate a single table '''

        tbl = define_table(self, name, columns, constraints)
        tbl.create(self._engine)

    def create_all(self):
        '''
        Instantiate all tables defined so far which don't already exist
        '''
        self.create_schema()
        self._metadata.create_all(self._engine)
        try:
            self.grant_reader_access('reg_reader')
        except:
            print("Could not grant access to reg_reader")

    def get_table_metadata(self, table_name):
        if not "." in table_name:
            table_name = ".".join([self._schema, table_name])
        return self._metadata.tables[table_name]

    def create_schema(self):
        if self._dialect == 'sqlite':
            return
        stmt = f'CREATE SCHEMA IF NOT EXISTS {self._schema}'
        with self._engine.connect() as conn:
            conn.execute(text(stmt))
            conn.commit()

    def grant_reader_access(self, acct):
        '''
        Grant USAGE on schema, SELECT on tables to specified account
        '''
        if self._dialect == 'sqlite':
            return
        # Cannot figure out how to pass value of acct using parameters,
        # so for safety do minimal checking ourselves: check that value of
        # acct has no spaces
        if len(acct.split()) != 1 :
            raise ValueException(f'grant_reader_access: {acct} is not a valid account')
        usage_priv = f'GRANT USAGE ON SCHEMA {self._schema} to {acct}'
        select_priv = f'GRANT SELECT ON ALL TABLES IN SCHEMA {self._schema} to {acct}'
        with self._engine.connect() as conn:
            conn.execute(text(usage_priv))
            conn.execute(text(select_priv))
            conn.commit()

class TableMetadata():
    '''
    Keep and dispense table metadata
    '''
    def __init__(self, schema, engine):
        self._metadata = MetaData(schema=schema)
        self._engine = engine
        self._schema = schema

        # Load all existing tables
        self._metadata.reflect(self._engine, schema)

        # Fetch and save db versioning if present
        prov_name = ".".join([schema, "provenance"])
        if prov_name in self._metadata.tables:
            prov_table = self._metadata.tables[prov_name]
            cols = ["db_version_major", "db_version_minor", "db_version_patch"]
            stmt=select(*[column(c) for c in cols])
            stmt=stmt.select_from(prov_table)
            stmt=stmt.order_by(prov_table.c.provenance_id.desc())
            with self._engine.connect() as conn:
                results = conn.execute(stmt)
                conn.commit()
            r = results.fetchone()
            self._db_major = r[0]
            self._db_minor = r[1]
            self._db_patch = r[2]
        else:
            self._db_major = None
            self._db_minor = None
            self._db_patch = None

    @property
    def db_version_major(self):
        return self._db_major

    @property
    def db_version_minor(self):
        return self._db_minor

    @property
    def db_version_patch(self):
        return self._db_patch

    def get(self, tbl):
        if "." not in tbl:
            tbl = ".".join([self._schema, tbl])
        if tbl not in self._metadata.tables.keys():
            try:
                self._metadata.reflect(self._engine, only=[tbl])
            except:
                raise ValueError(f'No such table {tbl}')

        return self._metadata.tables[tbl]
