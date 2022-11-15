from sqlalchemy import create_engine, MetaData, Table
import yaml
import os
from collections import namedtuple

__all__ = ['create_db_engine', 'TableCreator']

def create_db_engine(config_file=None, db_dialect='postgresql'):

    if config_file:
        # Should check file is private to user
        with open(config_file) as f:
            connection_parameters = yaml.safe_load(f)

        if db_dialect == 'postgresql':
            if not 'pwd' in connection_parameters:
                raise ValueException('Db connection file must contain value for pwd')
            pwd = connection_parameters['pwd']
            host = connection_parameters['host']
            dbname = connection_parameters['dbname']
            user = connection_parameters['user']
            if '5432' not in host:
                host = host + ':5432'

            url = f'{db_dialect}://{user}:{pwd}@{host}/{dbname}'
        elif db_dialect == 'sqlite':
            # dbname parameter should be path to sqlite file
            db_file = connection_parameters['dbname']
            url = f'{db_dialect}///{db_file}'

        return create_engine(url)

    else:
        # If postgres maybe use .pgpass?
        # check that .pgpass exists with correct permissions
        raise NotImplementedException('''Currently can only create engine from
                                         config file''')

class TableCreator:
    def __init__(self, engine, schema=None):
        self._engine = engine
        self._schema = schema
        self._metadata = MetaData(bind=engine, schema=schema)

    def define_table(self, name, columns):
        '''
        Usual case: caller wants to create a collection of tables all at once
        so just stash definition in MetaData object.

        Parameters
        name         string          table name
        columns      list of sqlalchemy.Column objects

        returns     tbl, an sqlalchemy.Table object.
                    User may instantiate immediately with
                    sqlalchemy.Table.create  method

        '''
        tbl = Table(name, self._metadata, *columns)

        return tbl

    def create_table(self, name, columns):
        ''' Define and instantiate a single table '''

        tbl = define_table(self, name, columns)
        tbl.create()

    def create_all(self):
        '''
        Instantiate all tables defined so far which don't already exist
        '''
        self._metadata.create_all()

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
