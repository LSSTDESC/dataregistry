import os
import time
from datetime import datetime

from dataregistry.db_basic import add_table_row
from sqlalchemy import select, update

from .registrar_util import (
    _bump_version,
    _copy_data,
    _form_dataset_path,
    _name_from_relpath,
    _parse_version_string,
    _read_configuration_file,
    get_directory_info,
)

# Default maximum allowed length of configuration file allowed to be ingested
_DEFAULT_MAX_CONFIG = 10000


class RegistrarDataset:
    def __init__(self, parent):
        """
        Wrapper class to register/modify/delete dataset entries.

        Parameters
        ----------
        parent : Registrar class
            Contains db_connection, engine, etc
        """

        self.parent = parent

    def create(
        self,
        relative_path,
        version,
        version_suffix=None,
        name=None,
        creation_date=None,
        description=None,
        execution_id=None,
        access_API=None,
        access_API_configuration=None,
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
        input_production_datasets=[],
        max_config_length=_DEFAULT_MAX_CONFIG,
    ):
        """
        Create a new dataset entry in the DESC data registry.

        Any args marked with '**' share their name with the associated column
        in the registry schema. Descriptions of what these columns are can be
        found in `schema.yaml` or the documentation.

        First, the dataset entry is created in the database. If success, the
        data is then copied (if `old_location` was provided). Only if both
        steps are successful will there be `status=1` entry in the registry.

        Parameters
        ----------
        relative_path** : str
        version** : str
        version_suffix** : str, optional
        name** : str, optional
        creation_date** : datetime, optional
        description** : str, optional
        execution_id** : int, optional
        access_API** : str, optional
        is_overwritable** : bool, optional
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
        owner** : str, optional
        owner_type** : str, optional
        execution_name** : str, optional
        execution_description** : str, optional
        execution_start** : datetime, optional
        execution_locale** : str, optional
        execution_configuration** : str, optional
        input_datasets : list, optional
            List of dataset ids that were the input to this execution
        input_production_datasets : list, optional
            List of production dataset ids that were the input to this execution
        max_config_length : int, optional
            Maxiumum number of lines to read from a configuration file

        Returns
        -------
        prim_key : int
            The dataset ID of the new row relating to this entry (else None)
        execution_id : int
            The execution ID associated with the dataset
        """

        # Make sure the owner_type is legal
        if owner_type is None:
            if self.parent._owner_type is not None:
                owner_type = self.parent._owner_type
            else:
                owner_type = "user"
        if owner_type not in self.parent.get_owner_types():
            raise ValueError(f"{owner_type} is not a valid owner_type")

        # Establish the dataset owner
        if owner is None:
            if self.parent._owner is not None:
                owner = self.parent._owner
            else:
                owner = self.parent._uid
        if owner_type == "production":
            owner = "production"

        # Checks for production datasets
        if owner_type == "production":
            if is_overwritable:
                raise ValueError("Cannot overwrite production entries")
            if version_suffix is not None:
                raise ValueError("Production entries can't have version suffix")
            if self.parent._schema != "production":
                raise ValueError(
                    "Only the production schema can handle owner_type='production'"
                )
        else:
            if self.parent._schema == "production":
                raise ValueError(
                    "Only the production schema can handle owner_type='production'"
                )

        # If `name` not passed, automatically generate a name from the relative path
        if name is None:
            name = _name_from_relpath(relative_path)

        # Look for previous entries. Fail if not overwritable
        dataset_table = self.parent._get_table_metadata("dataset")
        previous_dataset = self._find_previous(
            dataset_table,
            relative_path=relative_path,
            owner=owner,
            owner_type=owner_type,
        )

        if previous_dataset is not None:
            if not previous_dataset.is_overwritable:
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
                name, version, version_suffix, dataset_table, self.parent._engine
            )
            version_string = (
                f"{v_fields['major']}.{v_fields['minor']}.{v_fields['patch']}"
            )

        # If no execution_id is supplied, create a minimal entry
        if execution_id is None:
            if execution_name is None:
                execution_name = f"for_dataset_{name}-{version_string}"
                if version_suffix:
                    execution_name = f"{execution_name}-{version_suffix}"
            if execution_description is None:
                execution_description = "Fabricated execution for dataset"
            execution_id = self.parent.execution.create(
                execution_name,
                description=execution_description,
                execution_start=execution_start,
                locale=execution_locale,
                configuration=execution_configuration,
                input_datasets=input_datasets,
                input_production_datasets=input_production_datasets,
            )

        # Pull the dataset properties together
        values = {"name": name, "relative_path": relative_path}
        values["version_major"] = v_fields["major"]
        values["version_minor"] = v_fields["minor"]
        values["version_patch"] = v_fields["patch"]
        values["version_string"] = version_string
        if version_suffix:
            values["version_suffix"] = version_suffix
        if description:
            values["description"] = description
        if execution_id:
            values["execution_id"] = execution_id
        if access_API:
            values["access_API"] = access_API
        if access_API_configuration:
            values["access_API_configuration"] = _read_configuration_file(
                access_API_configuration, max_config_length
            )
        values["is_overwritable"] = is_overwritable
        values["is_overwritten"] = False
        values["is_external_link"] = False
        values["is_archived"] = False
        values["register_date"] = datetime.now()
        values["owner_type"] = owner_type
        values["owner"] = owner
        values["creator_uid"] = self.parent._uid
        values["register_root_dir"] = self.parent._root_dir

        # We tentatively start with an "invalid" dataset in the database. This
        # will be upgraded to valid if the data copying (if any) was successful.
        values["status"] = -1

        # Create a new row in the data registry database.
        with self.parent._engine.connect() as conn:
            prim_key = add_table_row(conn, dataset_table, values, commit=False)

            if previous_dataset is not None:
                # Update previous rows, setting is_overwritten to True
                update_stmt = (
                    update(dataset_table)
                    .where(dataset_table.c.dataset_id == previous_dataset.dataset_id)
                    .values(is_overwritten=True)
                )
                conn.execute(update_stmt)
            conn.commit()

        # Get dataset characteristics; copy to `root_dir` if requested
        if not is_dummy:
            (
                dataset_organization,
                num_files,
                total_size,
                ds_creation_date,
            ) = self._handle_data(
                relative_path, old_location, owner, owner_type, verbose
            )
            valid_status = 1
        else:
            dataset_organization = "dummy"
            num_files = 0
            total_size = 0
            ds_creation_date = None
            valid_status = 0

        # Case where use is overwriting the dateset `creation_date`
        if creation_date:
            ds_creation_date = creation_date

        # Copy was successful, update the entry with dataset metadata
        with self.parent._engine.connect() as conn:
            update_stmt = (
                update(dataset_table)
                .where(dataset_table.c.dataset_id == prim_key)
                .values(
                    data_org=dataset_organization,
                    nfiles=num_files,
                    total_disk_space=total_size / 1024 / 1024,
                    creation_date=ds_creation_date,
                    status=valid_status,
                )
            )
            conn.execute(update_stmt)
            conn.commit()

        return prim_key, execution_id

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
            owner_type,
            owner,
            relative_path,
            schema=self.parent._schema,
            root_dir=self.parent._root_dir,
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
            _copy_data(dataset_organization, old_location, dest)
            if verbose:
                print(f"took {time.time()-tic:.2f}")

        return dataset_organization, num_files, total_size, ds_creation_date

    def _find_previous(
        self,
        dataset_table,
        relative_path=None,
        owner=None,
        owner_type=None,
        dataset_id=None,
    ):
        """
        Check to see if a dataset exists already in the registry, and if we are
        allowed to overwrite it.

        Can search either by `dataset_id`, or a combination of `relative_path`,
        `owner` and `owner_type`.

        Only one dataset should ever be found.

        Parameters
        ----------
        dataset_table : SQLAlchemy Table object
            Link to the dataset table
        relative_path : str, optional
            Relative path to dataset
        owner : str, optional
            Owner of the dataset
        owner_type : str, optional
        dataset_id : int, optional

        Returns
        -------
        r : CursorResult object
            Searched dataset
        """

        # Make sure we have all the relavant information
        if dataset_id is None:
            if (relative_path is None) or (owner is None) or (owner_type is None):
                raise ValueError(
                    "Must pass relative_path, owner and owner_type to _find_previous"
                )

        # Search for dataset in the registry.
        if dataset_id is None:
            stmt = (
                select(
                    dataset_table.c.dataset_id,
                    dataset_table.c.is_overwritable,
                    dataset_table.c.status,
                    dataset_table.c.owner,
                    dataset_table.c.owner_type,
                    dataset_table.c.relative_path,
                )
                .where(
                    dataset_table.c.relative_path == relative_path,
                    dataset_table.c.owner == owner,
                    dataset_table.c.owner_type == owner_type,
                )
                .order_by(dataset_table.c.dataset_id.desc())
            )
        else:
            stmt = (
                select(
                    dataset_table.c.dataset_id,
                    dataset_table.c.is_overwritable,
                    dataset_table.c.status,
                    dataset_table.c.owner,
                    dataset_table.c.owner_type,
                    dataset_table.c.relative_path,
                )
                .where(
                    dataset_table.c.dataset_id == dataset_id,
                )
                .order_by(dataset_table.c.dataset_id.desc())
            )

        with self.parent._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # If the datasets are overwritable, log their ID, else return None
        for r in result:
            return r

        return None

    def delete(self, dataset_id):
        """
        Delete a dataset entry from the DESC data registry.

        This will remove the raw data from the root dir, but the dataset entry
        remains in the registry (now with `status=3`).

        Parameters
        ----------
        dataset_id : int
            Dataset we want to delete from the registry
        """

        # First make sure the given dataset id is in the registry
        dataset_table = self.parent._get_table_metadata("dataset")
        previous_dataset = self._find_previous(dataset_table, dataset_id=dataset_id)

        if previous_dataset is None:
            raise ValueError(f"Dataset ID {dataset_id} does not exist")
        if previous_dataset.status not in [0, 1]:
            raise ValueError(f"Dataset ID {dataset_id} does not have a valid status")

        # Update the status of the dataset to deleted
        with self.parent._engine.connect() as conn:
            update_stmt = (
                update(dataset_table)
                .where(dataset_table.c.dataset_id == dataset_id)
                .values(
                    status=3,
                    delete_date=datetime.now(),
                    delete_uid=self.parent._uid,
                )
            )
            conn.execute(update_stmt)
            conn.commit()

        # Delete the physical data in the root_dir
        if previous_dataset.status == 1:
            data_path = _form_dataset_path(
                previous_dataset.owner_type,
                previous_dataset.owner,
                previous_dataset.relative_path,
                schema=self.parent._schema,
                root_dir=self.parent._root_dir,
            )
            print(f"Deleting data {data_path}")
            os.remove(data_path)

        print(f"Deleted {dataset_id} from data registry")

    def modify(self):
        """
        Modify a dataset entry in the DESC data registry.

        """

        raise NotImplementedError
