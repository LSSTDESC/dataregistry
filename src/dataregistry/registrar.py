from datetime import datetime
from sqlalchemy import MetaData, Table, Column, insert   # maybe other stuff
from sqlalchemy.exc import DBAPIError
from dataregistry.db_basic import SCHEMA_VERSION, OwnershipEnum

class Registrar():
    '''
    Register new datasets, executions ("runs") or alias names
    '''
    def __init__(self, db_engine, owner_type, owner=None,
                 schema_version=SCHEMA_VERSION):
        self._engine = db_engine
        self._owner_type = owner_type
        self._owner = owner
        if self._owner_type == OwnershipEnum.production:
            self._owner = 'production'
        self._schema_version=schema_version
        self._metadata = MetaData(bind=db_engine, schema=self._schema_version)

    def register_dataset(self, name, relative_path, version_major,
                         version_minor,
                         version_patch, version_suffix=None, creation_date=None,
                         description=None, execution_id=None, access_API=None):
        # Create new row using
        #    arguments
        #    schema, owner_type, owner from self
        #    generate value for register_date
        pass # for now

    def register_execution(self, name, description=None, execution_start=None,
                           locale=None):
        '''
        Register an execution of something.   Return id

        Parameters
        ----------
        name            string   Typically pipeline name or program name
                                 (should there also be a version string?)
        program_version string   Optional
        nickname        string   Optional (Must be unique)
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

        exec_table = Table("execution", self._metadata,
                           autoload_with=self._engine)
        with self._engine.connect() as conn:
            try:
                result = conn.execute(insert(exec_table), [values])
                # It seems autocommit is in effect and no commit is needed
                ## conn.commit()
                return result[0]["execution_id"]
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None
