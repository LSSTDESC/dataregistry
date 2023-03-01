import os
from datetime import datetime
from sqlalchemy import MetaData, Table, Column, insert, text, update, select
from sqlalchemy.exc import DBAPIError
from dataregistry.db_basic import add_table_row, SCHEMA_VERSION, ownertypeenum
from dataregistry.registrar_util import form_dataset_path
from dataregistry.exceptions import *

__all__ = ['Registrar']
_DEFAULT_ROOT_DIR = '/global/cfs/cdirs/desc-co/jrbogart/dregs_root' #temporary
class Registrar():
    '''
    Register new datasets, executions ("runs") or alias names
    '''
    def __init__(self, db_engine, dialect, owner_type, owner=None,
                 schema_version=SCHEMA_VERSION):
        self._engine = db_engine
        self._dialect = dialect
        self._owner_type = owner_type.value
        if owner is None:
            owner = '.'
        self._owner = owner
        if self._owner_type == ownertypeenum.production.value:
            self._owner = 'production'
        if dialect == 'sqlite':
            self._schema_version = None
        else:
            self._schema_version=schema_version
        self._metadata = MetaData(schema=self._schema_version)
        self._userid = os.getenv('USER')
        self._dataset_table = None
        self._execution_table = None
        self._dataset_alias_table = None
        self._execution_alias_table = None
        self._dependency_table = None
        self._root_dir = _DEFAULT_ROOT_DIR  # <-- should be site dependent

    # Should verify that argument is reasonable       <---
    def set_root_dir(self, root_dir):
        self._root_dir = root_dir

    @property
    def root_dir(self):
        return self._root_dir

    def register_dataset(self, name, relative_path, version_major,
                         version_minor,
                         version_patch, version_suffix=None, creation_date=None,
                         description=None, execution_id=None, access_API=None,
                         is_overwritable=False, old_location=None, copy=True):
        '''
        Return id of new row if successful, else None

        Parameters
        ----------
        name             Any convenient, evocative name for the human.
                         No check for uniqueness. Perhaps should be uniqueness
                         constrain on (name, version_major, version_minor,
                                       version_patch)
        relative_path    Relative to owner_type/owner as specified to
                         to Registrar constructor
        version_major, version_minor, version_patch
                         Required version specifiers
        version_suffix   Optional version specifier for non-production datasets
        creation_date    If not specified, take from dataset file or directory
                         creation date
        description      Optional human-readable description
        execution_id     Optional; used to associate dataset with a particular
                         execution of some code
        access_API       Hint as to how to read the data
        is_overwritable  True if dataset may be overwritten.  Defaults to False.
                         Always False for production datasets
        old_location     Absolute location of dataset.  If None, dataset should
                         already be at correct relative_path
        copy             If old_location is None, ignore.   Else,
                           * if True copy from old_location to relative_path.
                           * if False make sym link at relative_path

        '''
        if (self._owner_type == 'production') and is_overwritable:
            raise ValueError('Cannot overwrite production entries')

        if old_location:
            raise DataRegistryNYI('dataset copy')

        if self._dataset_table is None:
            self._dataset_table = Table("dataset", self._metadata,
                                        autoload_with=self._engine)
        dataset_table = self._dataset_table
        # First check if any entries already exist with the same relative_path
        # and, if they do, if they are overwritable. If any are not, abort
        stmt = select(dataset_table.c.dataset_id,
                      dataset_table.c.is_overwritable)\
                      .where(dataset_table.c.relative_path == relative_path,
                             dataset_table.c.owner == self._owner,
                             dataset_table.c.owner_type == self._owner_type)\
                      .order_by(dataset_table.c.dataset_id.desc())
        previous = []
        with self._engine.connect() as conn:
            try:
                result = conn.execute(stmt)
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None

            for r in result:
                if not r.is_overwritable:
                    print(f'Dataset with relative path {relative_path} exists and is not overwritable')
                    return None
                else:
                    previous.append(r.dataset_id)

        # Confirm new dataset exists
        dest =  form_dataset_path(self._owner_type, self._owner,
                                  relative_path, self._root_dir)
        if old_location:
            loc = old_location
        else:
            loc = dest

        try:
            f = open(loc)
            f.close()
        except Exception as e:
            print('Dataset to be registered does not exist or is not readable')
            raise e

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
        values["is_overwritable"] = is_overwritable
        values["is_overwritten"] = False
        values["register_date"] = datetime.now()
        values["owner_type"] = self._owner_type
        values["owner"] = self._owner
        values["creator_uid"] = self._userid

        with self._engine.connect() as conn:
            prim_key = add_table_row(conn, dataset_table, values)
            if not prim_key:
                return None

            if len(previous) > 0:
                try:
                    # Update previous rows, setting is_overwritten to True
                    update_stmt = update(dataset_table)\
                      .where(dataset_table.c.dataset_id.in_(previous))\
                      .values(is_overwritten=True)
                    conn.execute(update_stmt)
                    conn.commit()
                except DBAPIError as e:
                    print('Original error:')
                    print(e.StatementError.orig)
                    return None

        # Not yet implemented
        # if old_location:
        #     # copy from loc to destination
        #     try   ...

        return prim_key

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
            return add_table_row(conn, exec_table, values)

    def register_dataset_alias(self, aliasname, dataset_id):
        '''
        Make an alias for an existing dataset

        Parameters
        ----------
        aliasname          the new alias
        dataset_id         id for existing dataset


        '''
        now = datetime.now()
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
                prim_key = add_table_row(conn, alias_table, values)

                # Update any other alias rows which have been superseded
                stmt = update(alias_table)\
                       .where(alias_table.c.alias == aliasname,
                              alias_table.c.dataset_alias_id != prim_key)\
                       .values(supersede_date=now)
                conn.execute(stmt)
                return prim_key
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None
