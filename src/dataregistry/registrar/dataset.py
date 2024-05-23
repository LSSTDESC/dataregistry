import os
import time
from datetime import datetime
import shutil

from dataregistry.db_basic import add_table_row
from sqlalchemy import select, update

from .base_table_class import BaseTable
from .registrar_util import (
    _bump_version,
    _copy_data,
    _form_dataset_path,
    _name_from_relpath,
    _parse_version_string,
    _read_configuration_file,
    get_directory_info,
)
from .dataset_util import set_dataset_status, get_dataset_status


class DatasetTable(BaseTable):
    def __init__(self, db_connection, root_dir, owner, owner_type, execution_table):
        super().__init__(db_connection, root_dir, owner, owner_type)

        self.execution_table = execution_table
        self.which_table = "dataset"
        self.entry_id = "dataset_id"

    def register(
        self,
        relative_path,
        version,
        version_suffix=None,
        name=None,
        creation_date=None,
        description=None,
        execution_id=None,
        access_api=None,
        access_api_configuration=None,
        is_overwritable=False,
        old_location=None,
        copy=True,
        verbose=False,
        owner=None,
        owner_type=None,
        execution_name=None,
        execution_description=None,
        execution_start=None,
        execution_site=None,
        execution_configuration=None,
        input_datasets=[],
        input_production_datasets=[],
        max_config_length=None,
        location_type="dataregistry",
        url=None,
        contact_email=None,
        test_production=False,
    ):
        """
        Create a new dataset entry in the DESC data registry.

        Any args marked with '**' share their name with the associated column
        in the registry schema. Descriptions of what these columns are can be
        found in `schema.yaml` or the documentation.

        First, the dataset entry is created in the database. If success, the
        data is then copied (if `old_location` was provided). Only if both
        steps are successful will there be "valid" status entry in the
        registry.

        Parameters
        ----------
        relative_path** : str
        version** : str
        version_suffix** : str, optional
        name** : str, optional
        creation_date** : datetime, optional
        description** : str, optional
        execution_id** : int, optional
        access_api** : str, optional
        is_overwritable** : bool, optional
        old_location : str, optional
            Absolute location of dataset to copy into the data registry.

            If None, dataset should already be at correct relative_path within
            the data registry.
        copy : bool, optional
            True to copy data from ``old_location`` into the data registry
            (default behaviour).
            False to create a symlink.
        verbose : bool, optional
            Provide some additional output information
        owner** : str, optional
        owner_type** : str, optional
        execution_name** : str, optional
        execution_description** : str, optional
        execution_start** : datetime, optional
        execution_site** : str, optional
        execution_configuration** : str, optional
        input_datasets : list, optional
            List of dataset ids that were the input to this execution
        input_production_datasets : list, optional
            List of production dataset ids that were the input to this execution
        max_config_length : int, optional
            Maxiumum number of lines to read from a configuration file
        location_type**: str, optional
            If `location_type="external"`, either `url` or `contact_email` must
            be supplied
        url**: str, optional
            For `location_type="external"` only
        contact_email**: str, optional
        test_production: boolean, default False.  Set to True for testing
                         code for production owner_type

        Returns
        -------
        prim_key : int
            The dataset ID of the new row relating to this entry (else None)
        execution_id : int
            The execution ID associated with the dataset
        """

        # If external dataset, check for either a `url` or `contact_email`
        if location_type == "external":
            if url is None and contact_email is None:
                raise ValueError("External datasets require either a url or contact_email")

        # Set max configuration file length
        if max_config_length is None:
            max_config_length = self._DEFAULT_MAX_CONFIG

        # Make sure the owner_type is legal
        if owner_type is None:
            if self._owner_type is not None:
                owner_type = self._owner_type
            else:
                owner_type = "user"
        if owner_type not in self._OWNER_TYPES:
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
            if self._schema != "production" and not test_production:
                raise ValueError(
                    "Only the production schema can handle owner_type='production'"
                )
        else:
            if self._schema == "production" or test_production:
                raise ValueError(
                    "Only owner_type='production' can go in the production schema"
                )

        # If `name` not passed, automatically generate a name from the relative path
        if name is None:
            name = _name_from_relpath(relative_path)

        # Look for previous entries. Fail if not overwritable
        dataset_table = self._get_table_metadata("dataset")
        previous = self._find_previous(relative_path, owner, owner_type)

        if previous is None:
            print(f"Dataset {relative_path} exists, and is not overwritable")
            return None, None

        # Deal with version string (non-special case)
        if version not in ["major", "minor", "patch"]:
            v_fields = _parse_version_string(version)
            version_string = version
        else:
            # Generate new version fields based on previous entries
            # with the same name field (i.e., bump)
            v_fields = _bump_version(name, version, dataset_table, self._engine)
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
            execution_id = self.execution_table.register(
                execution_name,
                description=execution_description,
                execution_start=execution_start,
                site=execution_site,
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
        if access_api:
            values["access_api"] = access_api
        if access_api_configuration:
            values["access_api_configuration"] = _read_configuration_file(
                access_api_configuration, max_config_length
            )
        values["is_overwritable"] = is_overwritable
        values["is_overwritten"] = False
        values["is_archived"] = False
        values["register_date"] = datetime.now()
        values["owner_type"] = owner_type
        values["owner"] = owner
        values["creator_uid"] = self._uid
        values["register_root_dir"] = self._root_dir
        values["location_type"] = location_type
        if url and location_type == "external":
            values["url"] = url
        if contact_email:
            values["contact_email"] = contact_email

        # We tentatively start with an "invalid" dataset in the database. This
        # will be upgraded to valid if the data copying (if any) was successful.
        values["status"] = 0

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

        # Get dataset characteristics; copy to `root_dir` if requested
        if location_type == "dataregistry":
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
            dataset_organization = location_type
            num_files = 0
            total_size = 0
            ds_creation_date = None
            valid_status = 0

        # Case where use is overwriting the dateset `creation_date`
        if creation_date:
            ds_creation_date = creation_date

        # Copy was successful, update the entry with dataset metadata
        with self._engine.connect() as conn:
            update_stmt = (
                update(dataset_table)
                .where(dataset_table.c.dataset_id == prim_key)
                .values(
                    data_org=dataset_organization,
                    nfiles=num_files,
                    total_disk_space=total_size / 1024 / 1024,
                    creation_date=ds_creation_date,
                    status=set_dataset_status(values["status"], valid=True),
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
            "file" or "directory"
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
            schema=self._schema,
            root_dir=self._root_dir,
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

    def _find_previous(self, relative_path, owner, owner_type):
        """
        Find each dataset with combination of `relative_path`, `owner`,
        `owner_type`.

        We want to know, of those datasets, which are overwritable but have not
        yet been marked as overwritten.

        If any dataset with the same path has `is_overwritable=False`, the
        routine returns None, indicating the dataset is not allowed to be
        overwritten.

        Parameters
        ----------
        relative_path : str
            Relative path to dataset
        owner : str
            Owner of the dataset
        owner_type : str
            Owner type of the dataset

        Returns
        -------
        dataset_id_list : list
            List of dataset IDs that have the desired path combination that are
            overwritable, but have not already previously been overwritten.
        """

        # Search for dataset in the registry.
        dataset_table = self._get_table_metadata("dataset")
        stmt = select(
            dataset_table.c.dataset_id,
            dataset_table.c.is_overwritable,
            dataset_table.c.is_overwritten,
        )

        stmt = stmt.where(
            dataset_table.c.relative_path == relative_path,
            dataset_table.c.owner == owner,
            dataset_table.c.owner_type == owner_type,
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # Pull out the single result
        dataset_id_list = []
        for r in result:
            if not r.is_overwritable:
                return None

            if not r.is_overwritten:
                dataset_id_list.append(r.dataset_id)

        return dataset_id_list

    def delete(self, dataset_id):
        """
        Delete an dataset entry from the DESC data registry.

        This will also remove the raw data from the root dir, but the dataset
        entry remains in the registry (now with an updated `status` field).

        Parameters
        ----------
        dataset_id : int
            Dataset we want to delete from the registry
        """

        # First make sure the given dataset id is in the registry
        dataset_table = self._get_table_metadata(self.which_table)
        previous_dataset = self.find_entry(dataset_id, raise_if_not_found=True)

        # Check dataset has not already been deleted
        if get_dataset_status(previous_dataset.status, "deleted"):
            raise ValueError(f"Dataset {dataset_id} has already been deleted")

        # Check dataset is valid
        if not get_dataset_status(previous_dataset.status, "valid"):
            raise ValueError(f"Dataset {dataset_id} is not a valid entry")

        # Update the status of the dataset to deleted
        with self._engine.connect() as conn:
            update_stmt = (
                update(dataset_table)
                .where(dataset_table.c.dataset_id == dataset_id)
                .values(
                    status=set_dataset_status(previous_dataset.status, deleted=True),
                    delete_date=datetime.now(),
                    delete_uid=self._uid,
                )
            )
            conn.execute(update_stmt)
            conn.commit()

        # Delete the physical data in the root_dir
        if previous_dataset.location_type == "dataregistry":
            data_path = _form_dataset_path(
                previous_dataset.owner_type,
                previous_dataset.owner,
                previous_dataset.relative_path,
                schema=self._schema,
                root_dir=self._root_dir,
            )
            print(f"Deleting data {data_path}")
            if os.path.isfile(data_path):
                os.remove(data_path)
            else:
                shutil.rmtree(data_path)

        print(f"Deleted {dataset_id} from data registry")
