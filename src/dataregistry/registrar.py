import time
import os
from datetime import datetime
from shutil import copyfile, copytree

# from sqlalchemy import MetaData, Table, Column, insert, text,
from sqlalchemy import update, select

# from sqlalchemy.exc import DBAPIError, IntegrityError
from dataregistry.db_basic import add_table_row
from dataregistry.registrar_util import _form_dataset_path, get_directory_info
from dataregistry.registrar_util import _parse_version_string, _bump_version
from dataregistry.registrar_util import _name_from_relpath
from dataregistry.db_basic import TableMetadata

# from dataregistry.exceptions import *

__all__ = ["Registrar"]

# Default maximum allowed length of configuration file allowed to be ingested
_DEFAULT_MAX_CONFIG = 10000

# Allowed owner types
_OWNER_TYPES = {"user", "project", "group", "production"}


class Registrar:
    def __init__(
        self,
        db_connection,
        root_dir,
        owner=None,
        owner_type=None,
    ):
        """
        Class to register new datasets, executions and alias names.

        Parameters
        ----------
        db_connection : DbConnection object
            Encompasses sqlalchemy engine, dialect (database backend)
            and schema version
        root_dir : str
            Root directory of the dataregistry on disk
        owner : str
            To set the default owner for all registered datasets in this
            instance.
        owner_type : str
            To set the default owner_type for all registered datasets in this
            instance.
        """

        # Root directory on disk for data registry files
        self._root_dir = root_dir

        # Database engine and dialect.
        self._engine = db_connection.engine
        self._schema = db_connection.schema

        # Link to Table Metadata.
        self._metadata_getter = TableMetadata(db_connection)

        # Store user id
        self._uid = os.getenv("USER")

        # Default owner and owner_type's
        self._owner = owner
        self._owner_type = owner_type

    def get_owner_types(self):
        """
        Returns a list of allowed owner_types that can be registered within the
        data registry.

        Returns
        -------
        - : set
            Set of owner_types
        """

        return _OWNER_TYPES

    def _get_table_metadata(self, tbl):
        return self._metadata_getter.get(tbl)

    def _find_previous(self, relative_path, dataset_table, owner, owner_type):
        """
        Check to see if a dataset exists already in the registry, and if we are
        allowed to overwrite it.

        Parameters
        ----------
        relative_path : str
            Relative path to dataset
        dataset_table : SQLAlchemy Table object
            Link to the dataset table
        owner : str
            Owner of the dataset
        owner_type : str

        Returns
        -------
        previous : list
            List of dataset IDs that are overwritable
        """

        # Search for dataset in the registry.
        stmt = (
            select(dataset_table.c.dataset_id, dataset_table.c.is_overwritable)
            .where(
                dataset_table.c.relative_path == relative_path,
                dataset_table.c.owner == owner,
                dataset_table.c.owner_type == owner_type,
            )
            .order_by(dataset_table.c.dataset_id.desc())
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # If the datasets are overwritable, log their ID, else return None
        previous = []
        for r in result:
            if not r.is_overwritable:
                return None
            else:
                previous.append(r.dataset_id)

        return previous

    def _handle_data(self, relative_path, old_location, owner, owner_type, verbose):
        """
        Find characteristics of dataset (i.e., is it a file or directory, how
        many files and total disk space of the dataset).

        If old_location is not None, copy the dataset files and directories
        into the data registry.

        Parameters
        ----------
        relative_path : str
            Relative path of dataset in the data registry
        old_location : str
            Location of data (if not already in the data registry root)
            Data will be copied from this location
        owner : str
            Owner of the dataset
        owner_type : str
            Owner type of the dataset
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
        ds_creation_date : datetime
            When file or directory was created
        """

        # Get destination directory in data registry.
        dest = _form_dataset_path(
            owner_type, owner, relative_path, root_dir=self._root_dir
        )

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

        ds_creation_date = datetime.fromtimestamp(os.path.getctime(loc))

        if dataset_organization == "directory":
            num_files, total_size = get_directory_info(loc)
        else:
            num_files = 1
            total_size = os.path.getsize(loc)
        if verbose:
            print(f"took {time.time()-tic:.2f}s")

        # Copy data into data registry
        if old_location:
            if verbose:
                tic = time.time()
                print(
                    f"Copying {num_files} files ({total_size/1024/1024:.2f} Mb)...",
                    end="",
                )
            if dataset_organization == "file":
                # Create any intervening directories
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                copyfile(old_location, dest)
            elif dataset_organization == "directory":
                copytree(old_location, dest, copy_function=copyfile)
            if verbose:
                print(f"took {time.time()-tic:.2f}")

        return dataset_organization, num_files, total_size, ds_creation_date

    def register_execution(
        self,
        name,
        description=None,
        execution_start=None,
        locale=None,
        configuration=None,
        input_datasets=[],
        input_production_datasets=[],
        max_config_length=_DEFAULT_MAX_CONFIG,
    ):
        """
        Register a new execution in the DESC data registry.

        Parameters
        ----------
        name : str
            Typically pipeline name or program name
        description : str, optional
            Human readible description of execution
        execution_start : datetime, optional
            Date the execution started
        locale : str, optional
            Where was the execution performed?
        configuration : str, optional
            Path to text file used to configure the execution
        input_datasets : list, optional
            List of dataset ids that were the input to this execution
        input_production_datasets : list, optional
            List of production dataset ids that were the input to this execution
        max_config_length : int, optional
            Maxiumum number of lines to read from a configuration file

        Returns
        -------
        my_id : int
            The execution ID of the new row relating to this entry
        """

        # Put the execution information together
        values = {"name": name}
        if locale:
            values["locale"] = locale
        if execution_start:
            values["execution_start"] = execution_start
        if description:
            values["description"] = description
        values["register_date"] = datetime.now()
        values["creator_uid"] = self._uid

        exec_table = self._get_table_metadata("execution")
        dependency_table = self._get_table_metadata("dependency")

        # Read configuration file. Enter contents as a raw string.
        if configuration:
            # Maybe first check that file size isn't outrageous?
            with open(configuration) as f:
                contents = f.read(max_config_length)
                # if len(contents) == _MAX_CONFIG:
                #    issue truncation warning?
            values["configuration"] = contents

        # Enter row into data registry database
        with self._engine.connect() as conn:
            my_id = add_table_row(conn, exec_table, values, commit=False)

            # handle dependencies
            for d in input_datasets:
                values["register_date"] = datetime.now()
                values["input_id"] = d
                values["execution_id"] = my_id
                add_table_row(conn, dependency_table, values, commit=False)

            # handle production dependencies
            for d in input_production_datasets:
                values["register_date"] = datetime.now()
                values["input_production_id"] = d
                values["execution_id"] = my_id
                add_table_row(conn, dependency_table, values, commit=False)

            conn.commit()
        return my_id

    def register_dataset(
        self,
        relative_path,
        version,
        version_suffix=None,
        name=None,
        creation_date=None,
        description=None,
        execution_id=None,
        access_API=None,
        is_overwritable=False,
        old_location=None,
        copy=True,
        is_dummy=False,
        verbose=False,
        owner=None,
        owner_type=None,
        execution_name=None,
        execution_description=None,
        execution_start=None,
        execution_locale=None,
        execution_configuration=None,
        input_datasets=[],
        input_production_datasets=[]
    ):
        """
        Register a new dataset in the DESC data registry.

        Parameters
        ----------
        relative_path : str
            Destination for the dataset within the data registry. Path is
            relative to ``<registry root>/<owner_type>/<owner>``.
        version : str
            Semantic version string of the format MAJOR.MINOR.PATCH *or*
            a special flag: "patch", "minor" or "major".

            When a special flag is used it automatically bumps the relative
            version for you (see examples for more details).
        version_suffix : str, optional
            Suffix string to place at the end of the version string. Cannot be
            used for production datasets.
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

            Note production datasets cannot be overwritten.
        old_location : str, optional
            Absolute location of dataset to copy into the data registry.

            If None, dataset should already be at correct relative_path within
            the data registry.
        copy : bool, optional
            True to copy data from ``old_location`` into the data registry
            (default behaviour).
            False to create a symlink.
        is_dummy : bool, optional
            True for "dummy" datasets (no data is copied, for testing purposes
            only)
        verbose : bool, optional
            Provide some additional output information
        owner : str, optional
            Owner of the dataset. If None, defaults to what was set in
            Registrar __init__, if that is also None, defaults to $USER.
        owner_type : str, optional
            Owner type: "user", "group", or "production". If None, defaults to
            what was set in Registrar __init__, if that is also None, defaults
            to "user".
        execution_name : str, optional
            Typically pipeline name or program name
        execution_description : str, optional
            Human readible description of execution
        execution_start : datetime, optional
            Date the execution started
        execution_locale : str, optional
            Where was the execution performed?
        execution_configuration : str, optional
            Path to text file used to configure the execution
        input_datasets : list, optional
            List of dataset ids that were the input to this execution
        input_production_datasets : list, optional
            List of production dataset ids that were the input to this execution

        Returns
        -------
        prim_key : int
            The dataset ID of the new row relating to this entry (else None)
        execution_id : int
            The execution ID associated with the dataset
        """

        # Make sure the owner_type is legal
        if owner_type is None:
            if self._owner_type is not None:
                owner_type = self._owner_type
            else:
                owner_type = "user"
        if owner_type not in _OWNER_TYPES:
            raise ValueError(f"{owner_type} is not a valid owner_type")

        # Establish the dataset owner
        if owner is None:
            if self._owner is not None:
                owner = self._owner
            else:
                owner = self._uid
        if owner_type == "production":
            owner = "production"

        # Checks for production datasets
        if owner_type == "production":
            if is_overwritable:
                raise ValueError("Cannot overwrite production entries")
            if version_suffix is not None:
                raise ValueError("Production entries can't have version suffix")
            if self._schema != "production":
                raise ValueError(
                    "Only the production schema can handle owner_type='production'"
                )
        else:
            if self._schema == "production":
                raise ValueError(
                    "Only the production schema can handle owner_type='production'"
                )

        # If name not passed, automatically generate a name from the relative path
        if name is None:
            name = _name_from_relpath(relative_path)

        # Look for previous entries. Fail if not overwritable
        dataset_table = self._get_table_metadata("dataset")
        previous = self._find_previous(relative_path, dataset_table, owner, owner_type)

        if previous is None:
            print(f"Dataset {relative_path} exists, and is not overwritable")
            return None

        # Deal with version string (non-special case)
        if version not in ["major", "minor", "patch"]:
            v_fields = _parse_version_string(version)
            version_string = version
        else:
            # Generate new version fields based on previous entries
            # with the same name field and same suffix (i.e., bump)
            v_fields = _bump_version(
                name, version, version_suffix, dataset_table, self._engine
            )
            version_string = (
                f"{v_fields['major']}.{v_fields['minor']}.{v_fields['patch']}"
            )

        # Get dataset characteristics; copy if requested
        if not is_dummy:
            (
                dataset_organization,
                num_files,
                total_size,
                ds_creation_date,
            ) = self._handle_data(
                relative_path, old_location, owner, owner_type, verbose
            )
        else:
            dataset_organization = "dummy"
            num_files = 0
            total_size = 0
            ds_creation_date = None

        # If no execution_id is supplied, create a minimal entry
        if execution_id is None:
            if execution_name is None:
                execution_name = f"for_dataset_{name}-{version_string}"
                if version_suffix:
                    execution_name = f"{execution_name}-{version_suffix}"
            if execution_description is None:
                execution_description = "Fabricated execution for dataset"
            execution_id = self.register_execution(
                execution_name,
                description=execution_description,
                execution_start=execution_start,
                locale=execution_locale,
                configuration=execution_configuration,
                input_datasets=input_datasets,
                input_production_datasets=input_production_datasets
            )

        # Pull the dataset properties together
        values = {"name": name}
        values["relative_path"] = relative_path
        values["version_major"] = v_fields["major"]
        values["version_minor"] = v_fields["minor"]
        values["version_patch"] = v_fields["patch"]
        values["version_string"] = version_string
        if version_suffix:
            values["version_suffix"] = version_suffix
        if creation_date:
            values["dataset_creation_date"] = creation_date
        else:
            if ds_creation_date:
                values["dataset_creation_date"] = ds_creation_date
        if description:
            values["description"] = description
        if execution_id:
            values["execution_id"] = execution_id
        if access_API:
            values["access_API"] = access_API
        values["is_overwritable"] = is_overwritable
        values["is_overwritten"] = False
        values["register_date"] = datetime.now()
        values["owner_type"] = owner_type
        values["owner"] = owner
        values["creator_uid"] = self._uid
        values["data_org"] = dataset_organization
        values["nfiles"] = num_files
        values["total_disk_space"] = total_size / 1024 / 1024  # Mb
        values["register_root_dir"] = self._root_dir

        # Create a new row in the data registry database.
        with self._engine.connect() as conn:
            prim_key = add_table_row(conn, dataset_table, values, commit=False)

            if len(previous) > 0:
                # Update previous rows, setting is_overwritten to True
                update_stmt = (
                    update(dataset_table)
                    .where(dataset_table.c.dataset_id.in_(previous))
                    .values(is_overwritten=True)
                )
                conn.execute(update_stmt)
            conn.commit()

        return prim_key, execution_id

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
        values = {"alias": aliasname}
        values["dataset_id"] = dataset_id
        values["register_date"] = now
        values["creator_uid"] = self._uid

        alias_table = self._get_table_metadata("dataset_alias")
        with self._engine.connect() as conn:
            prim_key = add_table_row(conn, alias_table, values)

            # Update any other alias rows which have been superseded
            stmt = (
                update(alias_table)
                .where(
                    alias_table.c.alias == aliasname,
                    alias_table.c.dataset_alias_id != prim_key,
                )
                .values(supersede_date=now)
            )
            conn.execute(stmt)
            conn.commit()
        return prim_key
