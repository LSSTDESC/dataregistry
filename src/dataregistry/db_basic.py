from sqlalchemy import create_engine, engine_from_config, MetaData, Table, text
import yaml
import os
import enum
from collections import namedtuple

SCHEMA_VERSION = 'registry_0_1'

__all__ = ['create_db_engine', 'TableCreator', 'SCHEMA_VERSION',
           'OwnershipEnum']

def create_db_engine(config_file, db_dialect='postgresql'):

    if config_file:
        # Should check file is private to user
        with open(config_file) as f:
            connection_parameters = yaml.safe_load(f)
            return engine_from_config(connection_parameters)

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

    cols = []
    cols.append(Column("primary_id", Integer, primary_key=True))
    cols.append(Column("short_string", String(16)))

    engine = create_db_engine(config_file=os.path.join(os.getenv('HOME'),
                                                       '.registry_config_dev'))

    tab_creator = TableCreator(engine, 'registry_0_1')

    tab_creator.define_table('sillytable', cols)

    tab_creator.create_all()
