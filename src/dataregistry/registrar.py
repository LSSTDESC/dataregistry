import time
import os
from datetime import datetime
from shutil import copyfile, copytree
from sqlalchemy import MetaData, Table, Column, insert, text, update, select
from sqlalchemy.exc import DBAPIError, IntegrityError
from dataregistry.db_basic import add_table_row, SCHEMA_VERSION, ownertypeenum
from dataregistry.registrar_util import _form_dataset_path, get_directory_info
from dataregistry.registrar_util import _parse_version_string, _bump_version
from dataregistry.registrar_util import _name_from_relpath
from dataregistry.db_basic import TableMetadata
from dataregistry.exceptions import *

__all__ = ['Registrar']
if os.getenv("DREGS_ROOT_DIR"):
    _DEFAULT_ROOT_DIR = os.getenv("DREGS_ROOT_DIR")
else:
    _DEFAULT_ROOT_DIR = "/global/cfs/cdirs/desc-co/registry-beta" #temporary

class Registrar():
    '''
    Register new datasets, executions ("runs") or alias names

    '''
    def __init__(self, db_engine, dialect, owner_type, owner=None,
                 schema_version=SCHEMA_VERSION):
        '''
        Create a new Registrar object. Note this call should be preceded
        by a call to create_db_engine, which will return values for
        db_engine and dialect.

        Parameters
        ----------
        db_engine :   sqlalchemy engine object
        dialect : str
            identifies target db type (e.g. "postgresql")
        owner_type : owenertypeenum
            which of the allowed categories will be destination for
            new dataset entries
        owner : str
            Forms part of relative path of dataset location.
            Always "production" for production databases.
            Otherwise defaults to "."
        schema_version : str
            Which database schema to connect to.
            Current default is 'registry_beta'
        '''
        self._engine = db_engine
        self._dialect = dialect
        self._owner_type = owner_type.value
        if owner is None:
            owner = "."
        self._owner = owner
        if self._owner_type == ownertypeenum.production.value:
            self._owner = "production"
        if dialect == "sqlite":
            self._schema_version = None
        else:
            self._schema_version=schema_version

        self._metadata_getter = TableMetadata(self._schema_version,
                                              db_engine)
        self._userid = os.getenv("USER")
        self._root_dir = _DEFAULT_ROOT_DIR  # <-- should be site dependent

    def _get_table_metadata(self, tbl):
        return self._metadata_getter.get(tbl)

    # Should verify that argument is reasonable       <---
    def set_root_dir(self, root_dir):
        self._root_dir = root_dir

    @property
    def root_dir(self):
        return self._root_dir

    def _find_previous(self, relative_path, dataset_table):
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
            result = conn.execute(stmt)
            conn.commit()

            for r in result:
                if not r.is_overwritable:
                    return None
                else:
                    previous.append(r.dataset_id)
        return previous

    def _handle_data(self, relative_path, old_location, verbose):
        """
        Find characteristics of dataset (i.e., is it a file or directory, how
        many files and total disk space of the dataset).

        If old_location is not None, copy the dataset into the data registry.

        Parameters
        ----------
        relative_path : str
            Relative path of dataset in the data registry
        old_location : str
            Location of data (if not already in the data registry root)
            Data will be copied from this location
        verbose : bool
            True for extra output

        Returns
        -------
        dataset_organization : str
            "file", "directory", or "dummy"
        num_files : int
            Total number of files making up dataset
        total_size : float
            Total disk space of dataset in bytes
        """

        # Get destination directory in data registry.
        dest =  _form_dataset_path(self._owner_type, self._owner,
                                   relative_path, self._root_dir)

        # Is the data already on location, or coming from somewhere new?
        if old_location:
            loc = old_location
        else:
            loc = dest

        # Get metadata on dataset.
        if os.path.isfile(loc):
            dataset_organization = "file"
        elif os.path.isdir(loc):
            dataset_organization = "directory"
        else:
            raise FileNotFoundError(f"Dataset {loc} not found")

        if verbose:
            tic = time.time()
            print("Collecting metadata...", end="")
        if dataset_organization == "directory":
            num_files, total_size = get_directory_info(loc)
        else:
            num_files = 1
            total_size = os.path.getsize(loc)
        if verbose:
            print(f"took {time.time()-tic:.2f}s")

        if old_location:
            # copy to dest.  For directory do recursive copy
            # for now always copy; don't try to handle sym link
            # Assuming we don't want to copy any metadata (e.g.
            # permissions)
            if verbose:
                tic = time.time()
                print(f"Copying {num_files} files ({total_size/1024/1024:.2f} Mb)...",end="")
            if dataset_organization == "file":
                # Create any intervening directories
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                copyfile(old_location, dest)
            elif dataset_organization == "directory":
                copytree(old_location, dest, copy_function=copyfile)
            if verbose:
                print(f"took {time.time()-tic:.2f}")

        return dataset_organization, num_files, total_size

    _MAX_CONFIG = 10000
    def register_execution(self, name, description=None, execution_start=None,
                           locale=None, configuration=None, input_datasets=[]):
        """
        Register a new execution in the DESC data registry.

        Parameters
        ----------
        name : str
            Typically pipeline name or program name
        description : str, optional
            Human readible description of execution
        execution_start : datetime, optional
            FILL IN
        locale : str, optional
            Where was the execution performed?
        configuration : str, optional
            Path to text file used to configure the execution
        input_datasets : list, optional
            List of dataset ids that were the input to this execution

        Returns
        -------
        my_id : int
            The execution ID of the new row relating to this entry
        """

        values = {"name" : name}
        if locale: values["locale"] = locale
        if execution_start: values["execution_start"] = execution_start
        if description: values["description"] = description
        values["register_date"] = datetime.now()
        values["creator_uid"] = self._userid

        exec_table = self._get_table_metadata("execution")
        dependency_table = self._get_table_metadata("dependency")

        if configuration:   # read text file into a string
            # Maybe first check that file size isn't outrageous?
            with open(configuration) as f:
                contents = f.read(Registrar._MAX_CONFIG)
                # if len(contents) == _MAX_CONFIG:
                #    issue truncation warning?
            values["configuration"] = contents

        with self._engine.connect() as conn:
            my_id = add_table_row(conn, exec_table, values, commit=False)

            # handle dependencies
            for d in input_datasets:
                values["register_date"] = datetime.now()
                values["input_id"] = d
                values["execution_id"] = my_id
                add_table_row(conn, dependency_table, values, commit=False)
            conn.commit()
        return my_id

    def register_dataset(self, relative_path, version,
                         version_suffix=None, name=None, creation_date=None,
                         description=None, execution_id=None,
                         access_API=None,
                         is_overwritable=False, old_location=None, copy=True,
                         is_dummy=False, verbose=False):
        """
        Register a new dataset in the DESC data registry.

        Parameters
        ----------
        relative_path : str
            Destination for the dataset within the data registry. Path is
            relative to ``<registry root>/<owner_type>/<owner>``.
            If the environment variable DREGS_ROOT is defined, this
            value is used for ``<registry root>``.
            Otherwise currently ``<registry root>`` defaults to
            /global/cfs/cdirs/desc-co/registry-beta
        version : str
            Semantic version string of the format MAJOR.MINOR.PATCH *or*
            a special flag "patch", "minor" or "major".

            When a special flag is used it automatically bumps the relative
            version for you (see examples for more details).
        version_suffix : str, optional
            Optional suffix string to place at the end of the version string.
            Cannot be used for production datasets.
        name : str, optional
            Any convenient, evocative name for the human.

            Note the combination of name, version and version_suffix must be
            unique.
        creation_date : datetime, optional
            Manually set creation date of dataset
        description : str, optional
            Human-readable description of dataset
        execution_id : int, optional
            Used to associate dataset with a particular execution
        access_API : str, optional
            Hint as to how to read the data
        is_overwritable : bool, optional
            True if dataset may be overwritten (defaults to False).

            Production datasets cannot be overwritten.
        old_location : str, optional
            Absolute location of dataset to copy.

            If None dataset should already be at correct relative_path.
        copy : bool, optional
            If true copy data from ``old_location`` to the database.
            If False create a symlink (defaults to True).
        is_dummy : bool
            True for "dummy" datasets (no data is copied, for testing purposes
            only)
        verbose : bool
            Provide some additional output information

        Returns
        -------
        prim_key : int
            The dataset ID of the new row relating to this entry (else None)
        """

        if (self._owner_type == "production"):
            if is_overwritable:
                raise ValueError("Cannot overwrite production entries")
            if version_suffix is not None:
                raise ValueError("Production entries can't have version suffix")

        dataset_table = self._get_table_metadata("dataset")

        if name is None:
            name = _name_from_relpath(relative_path)

        # Look for previous entries. Fail if not overwritable
        previous = self._find_previous(relative_path, dataset_table)
        if previous is None:
            print(f"Dataset with relative path {relative_path} exists and is not overwritable")
            return None

        # deal with version string
        special = version in ["major", "minor", "patch"]
        if not special:
            v_fields = _parse_version_string(version)
            version_string = version
        else:
            # Generate new version fields based on previous entries
            # with the same name field and same suffix
            v_fields = _bump_version(name, version, version_suffix,
                                     dataset_table, self._engine)
            version_string = f"{v_fields['major']}.{v_fields['minor']}.{v_fields['patch']}"

        # Get dataset characteristics; copy if requested
        if not is_dummy:
            dataset_organization, num_files, total_size = \
                self._handle_data(relative_path, old_location, verbose)
        else:
            dataset_organization = "dummy"
            num_files = 0
            total_size = 0

        # If no execution_id is supplied, create a minimal entry
        if execution_id is None:
            ex_name = f"for_dataset_{name}-{version_string}"
            if version_suffix:
                ex_name = f"{ex_name}-{version_suffix}"
            descr = "Fabricated execution for dataset"
            execution_id = self.register_execution(ex_name, description=descr)
            if execution_id is None:
                return None

        values  = {"name" : name}
        values["relative_path"] = relative_path
        values["version_major"] = v_fields["major"]
        values["version_minor"] = v_fields["minor"]
        values["version_patch"] = v_fields["patch"]
        values["version_string"] = version_string
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
        values["data_org"] = dataset_organization
        values["nfiles"] = num_files
        values["total_disk_space"] = total_size / 1024 / 1024 # Mb

        with self._engine.connect() as conn:
            prim_key = add_table_row(conn, dataset_table, values,
                                     commit=False)

            if len(previous) > 0:
                # Update previous rows, setting is_overwritten to True
                update_stmt = update(dataset_table)\
                    .where(dataset_table.c.dataset_id.in_(previous))\
                    .values(is_overwritten=True)
                conn.execute(update_stmt)
            conn.commit()

        return prim_key

    def register_dataset_alias(self, aliasname, dataset_id):
        """
        Register a new dataset alias in the DESC data registry.

        Parameters
        ----------
        aliasname : str
            Human readible alias for the dataset
        dataset_id : int
            Existing dataset ID to attach dataset alias to

        Returns
        -------
        prim_key : int
            The dataset_alias ID of the new row relating to this entry
        """

        now = datetime.now()
        values = {"alias" : aliasname}
        values["dataset_id"] = dataset_id
        values["register_date"] = now
        values["creator_uid"] = self._userid

        alias_table = self._get_table_metadata("dataset_alias")
        with self._engine.connect() as conn:
            prim_key = add_table_row(conn, alias_table, values)

            # Update any other alias rows which have been superseded
            stmt = update(alias_table)\
                .where(alias_table.c.alias == aliasname,
                       alias_table.c.dataset_alias_id != prim_key)\
                .values(supersede_date=now)
            conn.execute(stmt)
            conn.commit()
        return prim_key
