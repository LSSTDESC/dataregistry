from sqlalchemy import engine_from_config
from sqlalchemy.engine import make_url
from sqlalchemy import MetaData, Table, text
import yaml
import os
import enum
from collections import namedtuple

SCHEMA_VERSION = 'registry_0_1'

__all__ = ['create_db_engine', 'TableCreator', 'SCHEMA_VERSION',
           'OwnershipEnum']

def create_db_engine(config_file):
    # Ideally config_file does not contain password, but if it does
    # it should be accessible to owner only
    with open(config_file) as f:
        connection_parameters = yaml.safe_load(f)
        driver = make_url(connection_parameters['sqlalchemy.url']).drivername
        dialect = driver.split('+')[0]

        return engine_from_config(connection_parameters), dialect

class OwnershipEnum(enum.Enum):
    production = 1
    group = 2
    user = 3

class TableCreator:
    def __init__(self, engine, schema=SCHEMA_VERSION):
        self._engine = engine
        self._schema = schema
        self._metadata = MetaData(bind=engine, schema=schema)

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
        tbl.create()

    def create_all(self):
        '''
        Instantiate all tables defined so far which don't already exist
        '''
        self._metadata.create_all()

    def grant_reader_access(self, acct):
        '''
        Grant SELECT to specified account
        '''
        stmt = f'GRANT SELECT ON ALL TABLES IN SCHEMA {self._schema} to {acct}'
        with self._engine.connect() as conn:
            conn.execute(text(stmt))

if __name__ == '__main__':
    from sqlalchemy import Column, Integer, String
    import sys

    cols = []
    cols.append(Column("primary_id", Integer, primary_key=True))
    cols.append(Column("short_string", String(16)))

    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = os.path.join(os.getenv('HOME'), '.config_reg_writer')

    engine, dialect = create_db_engine(config_file=config_file)

    if dialect != 'sqlite':
        tab_creator = TableCreator(engine, schema='registry_0_1')
    else:
        tab_creator = TableCreator(engine, schema=None)

    tab_creator.define_table('sillytable', cols)

    tab_creator.create_all()
