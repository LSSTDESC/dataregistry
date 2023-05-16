import os
from datetime import datetime
from shutil import copyfile, copytree
from sqlalchemy import MetaData, Table, Column, insert, text, update, select
from sqlalchemy.exc import DBAPIError
from dataregistry.db_basic import add_table_row, SCHEMA_VERSION, ownertypeenum
from dataregistry.db_basic import TableMetadata
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

        self._metadata_getter = TableMetadata(self._schema_version,
                                              db_engine)
        self._userid = os.getenv('USER')
        self._root_dir = _DEFAULT_ROOT_DIR  # <-- should be site dependent

    def _get_table_metadata(self, tbl):
        return self._metadata_getter.get(tbl)

    # Should verify that argument is reasonable       <---
    def set_root_dir(self, root_dir):
        self._root_dir = root_dir

    @property
    def root_dir(self):
        return self._root_dir

    def register_dataset(self, name, relative_path, version_major,
                         version_minor,
                         version_patch, version_suffix=None, creation_date=None,
                         description=None, execution_id=None,
                         input_datasets=[], access_API=None,
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
        input_datasets   Iterable of dataset ids
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

        dataset_table = self._get_table_metadata("dataset")

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
                conn.commit()
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

        if old_location:
            # copy to dest.  For directory do recursive copy
            # for now always copy; don't try to handle sym link
            # Assuming we don't want to copy any metadata (e.g.
            # permissions)
            ftype = os.stat(old_location).st_mode & 0o0170000
            if ftype == 0o0100000:        # regular file
                copyfile(old_location, dest)
            elif fytpe == 0o0040000:      # directory
                copytree(old_location, dest, copy_function=copyfile)

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

            try:
                if len(previous) > 0:
                    # Update previous rows, setting is_overwritten to True
                    update_stmt = update(dataset_table)\
                      .where(dataset_table.c.dataset_id.in_(previous))\
                      .values(is_overwritten=True)
                    conn.execute(update_stmt)
                values = {"output_id" : prim_key}
                for d in input_datasets:
                    values["register_date"] = datetime.now()
                    values["input_id"] = d
                    add_table_row(conn, self._dependency_table, values)
                conn.commit()
            except DBAPIError as e:
                print('Original error:')
                print(e.StatementError.orig)
                return None

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

        exec_table = self._get_table_metadata("execution")
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

        alias_table = self._get_table_metadata("dataset_alias")
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
