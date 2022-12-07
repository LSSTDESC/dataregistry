import os
from datetime import datetime
from sqlalchemy import MetaData, Table, Column, insert, text   # maybe other stuff
from sqlalchemy.exc import DBAPIError
from dataregistry.db_basic import SCHEMA_VERSION, ownertypeenum

_SEPARATOR = '.'
def make_version_string(major, minor=0, patch=0, suffix=None):
    version = _SEPARATOR.join([major, minor, patch])
    if suffix:
        version = SEPARATOR.join([version, suffix])
    return version

def parse_version_string(version):
    '''
    Return dict with keys major, minor, patch and (if present) suffix
    '''
    # Perhaps better to do with regular expressions.  Or at least verify
    # that major, minor, patch are integers
    cmp = version.split(_SEPARATOR, maxsplit=3)
    d = {'major' : cmp[0]}
    if len(cmp) > 1:
        d['minor'] = cmp[1]
    else:
        d['minor'] = 0
    if len(cmp) > 2:
        d['patch'] = cmp[2]
    else:
        d['patch'] = 0
    if len(cmp) > 3:
        d['suffix'] = cmp[4]

    return d

class Registrar():
    '''
    Register new datasets, executions ("runs") or alias names
    '''
    def __init__(self, db_engine, dialect, owner_type, owner=None,
                 schema_version=SCHEMA_VERSION):
        self._engine = db_engine
        self._dialect = dialect
        self._owner_type = owner_type.value
        self._owner = owner
        if self._owner_type == ownertypeenum.production:
            self._owner = 'production'
        if dialect == 'sqlite':
            self._schema_version = None
        else:
            self._schema_version=schema_version
        self._metadata = MetaData(bind=db_engine, schema=self._schema_version)
        self._userid = os.getenv('USER')
        self._dataset_table = None
        self._execution_table = None
        self._dataset_alias_table = None
        self._execution_alias_table = None
        self._dependency_table = None

    def register_dataset(self, name, relative_path, version_major,
                         version_minor,
                         version_patch, version_suffix=None, creation_date=None,
                         description=None, execution_id=None, access_API=None):
        # Create new row using
        #    arguments
        #    schema, owner_type, owner from self
        #    generate value for register_date
        values  = {"name" : name}
        values["relative_path"] = relative_path
        values["version_major"] = version_major
        values["version_minor"] = version_minor
        values["version_patch"] = version_patch
        if version_suffix: values["version_suffix"] = version_suffix
        if creation_date: values["dataset_creation_date"] = creation_date
        if description: values["description"] = description
        if execution_id: values["execution_id"] = execution_id
        if access_API: values["access_API"] = access_API
        values["register_date"] = datetime.now()
        values["owner_type"] = self._owner_type
        values["owner"] = self._owner
        values["creator_uid"] = self._userid

        if self._dataset_table is None:
            self._dataset_table = Table("dataset", self._metadata,
                                        autoload_with=self._engine)
        dataset_table = self._dataset_table
        with self._engine.connect() as conn:
            try:
                result = conn.execute(insert(dataset_table), [values])
                # It seems autocommit is in effect and no commit is needed
                ## conn.commit()
                return result.inserted_primary_key[0]
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None

    def register_execution(self, name, description=None, execution_start=None,
                           locale=None):
        '''
        Register an execution of something.   Return id

        Parameters
        ----------
        name            string   Typically pipeline name or program name
                                 (should there also be a version string?)
        description     string   Optional
        execution_start datetime Optional
        locale          string   Optional

        '''
        # similar to register_dataset

        values = {"name" : name}
        if locale: values["locale"] = locale
        if execution_start: values["execution_start"] = execution_start
        if description: values["description"] = description
        values["register_date"] = datetime.now()
        values["creator_uid"] = self._userid

        if self._execution_table is None:

            self._execution_table = Table("execution", self._metadata,
                                          autoload_with=self._engine)
        exec_table = self._execution_table
        with self._engine.connect() as conn:
            try:
                result = conn.execute(insert(exec_table), [values])
                # It seems autocommit is in effect and no commit is needed
                ## conn.commit()
                return result.inserted_primary_key[0]
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None

    def register_dataset_alias(self, aliasname, dataset_id):
        '''
        Make an alias for an existing dataset

        Parameters
        ----------
        aliasname          the new alias
        dataset_id         id for existing dataset


        '''
        now = datatime.now()
        values = {"alias" : aliasname}
        values["dataset_id"] = dataset_id
        values["register_date"] = now
        values["creator_uid"] = self._userid

        if self._dataset_alias_table is None:

            self._dataset_alias_table = Table("dataset_alias", self._metadata,
                                              autoload_with=self._engine)
        alias_table = self._dataset_alias_table
        with self._engine.connect() as conn:
            try:
                result = conn.execute(insert(alias_table), [values])
                res =  result.inserted_primary_key[0]
                # Update any other alias rows which have been superseded
                stmt = update(alias_table)\
                       .where(alias_table.c.alias == aliasname,
                              id != res)\
                       .values(supersede_date=now)
                conn.execute(stmt)
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None
