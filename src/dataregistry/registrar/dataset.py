import inspect
import os
import time
from datetime import datetime
import shutil
from functools import wraps

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
    _relpath_from_name,
)
from .dataset_util import set_dataset_status, get_dataset_status

_ILLEGAL_NAME_CHAR = ["$", "*", "&", "/", "?", "\\", " "]


class DatasetTable(BaseTable):
    def __init__(self, db_connection, root_dir, owner, owner_type, execution_table):
        super().__init__(db_connection, root_dir, owner, owner_type)

        self.execution_table = execution_table
        self.which_table = "dataset"
        self.entry_id = "dataset_id"

        # Does the root_dir exist?
        self.root_dir_exists = os.path.isdir(root_dir)

        # Does the user have write permission to the root_dir?
        self.root_dir_write_access = os.access(root_dir, os.W_OK)

    def register(self, *args, **kwargs):
        kwargs["caller_function"] = "register"
        return self._register_or_replace(*args, **kwargs)

    def replace(self, *args, **kwargs):
        kwargs["caller_function"] = "replace"
        return self._register_or_replace(*args, **kwargs)

    def _validate_register_inputs(
        self,
        name,
        version,
        version_suffix,
        owner,
        owner_type,
        location_type,
        url,
        contact_email,
        is_overwritable,
        test_production,
    ):
        """
        An internal helper function to ensure the inputs to the register
        function are valid.

        If something is invalid, an exception is raised.

        Parameters
        ----------
        See `register()` function

        Returns
        -------
        owner : str
        owner_type : str
            These may have been updated from the global owner/owner_type
        """

        # If the root_dir does not exist, stop
        if not self.root_dir_exists:
            raise FileNotFoundError(f"root_dir {self._root_dir} does not exist")

        # `name` and `version` are mandatory, and should be strings
        if name is None or version is None:
            raise ValueError("A valid `name` and `version` are required")
        for att in [name, version]:
            if type(att) != str:
                raise ValueError(f"{att} is not a valid string")

        # Make sure `name` is legal (i.e., no illegal characters)
        for i_char in _ILLEGAL_NAME_CHAR:
            if i_char in name:
                raise ValueError(f"Cannot have character {i_char} in name string")

        # If external dataset, check for either a `url` or `contact_email`
        if location_type == "external":
            if url is None and contact_email is None:
                raise ValueError(
                    "External datasets require either a url or contact_email"
                )

        # Assign the `owner_type`
        if owner_type is None:
            if self._owner_type is not None:
                owner_type = self._owner_type
            else:
                owner_type = "user"

        # Assign the `owner`
        if owner is None:
            if self._owner is not None:
                owner = self._owner
            else:
                owner = self._uid

        # Make sure owner type is valid
        if owner_type not in self._OWNER_TYPES:
            raise ValueError(f"{owner_type} is not a valid owner_type")

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

            # The only owner allowed for production datasets is "production"
            if owner != "production":
                raise ValueError("`owner` for production datasets must be 'production'")
        else:
            if self._schema == "production" or test_production:
                raise ValueError(
                    "Only owner_type='production' can go in the production schema"
                )

        return owner, owner_type

    def _compute_version_string(self, name, version):
        """
        Compute version string (either manually, or from bumping)

        Parameters
        ----------
        name : str
        version : str

        Returns
        -------
        v_fields : dict
        version_string : str
        """

        # Deal with version string (non-special case)
        if version not in ["major", "minor", "patch"]:
            v_fields = _parse_version_string(version)
            version_string = version
        else:
            # Generate new version fields based on previous entries
            # with the same name field (i.e., bump)
            v_fields = _bump_version(
                name, version, self._get_table_metadata("dataset"), self._engine
            )
            version_string = (
                f"{v_fields['major']}.{v_fields['minor']}.{v_fields['patch']}"
            )

        return v_fields, version_string

    def _register_or_replace(
        self,
        name,
        version,
        version_suffix=None,
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
        keywords=[],
        location_type="dataregistry",
        url=None,
        contact_email=None,
        test_production=False,
        relative_path=None,
        caller_function=None,
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
        name** : str
        version** : str
        version_suffix** : str, optional
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
        keywords : list[str], optional
            List of keywords to tag dataset with.
            Each keyword must be registered already in the keywords table.
        location_type**: str, optional
            If `location_type="external"`, either `url` or `contact_email` must
            be supplied
        url**: str, optional
            For `location_type="external"` only
        contact_email**: str, optional
        test_production: boolean, default False.  Set to True for testing
                         code for production owner_type
        relative_path** : str, optional

        Returns
        -------
        prim_key : int
            The dataset ID of the new row relating to this entry (else None)
        execution_id : int
            The execution ID associated with the dataset
        """

        # Validate the inputs we are working with
        owner, owner_type = self._validate_register_inputs(
            name,
            version,
            version_suffix,
            owner,
            owner_type,
            location_type,
            url,
            contact_email,
            is_overwritable,
            test_production,
        )

        # Validate the keywords (make sure they are registered)
        if len(keywords) > 0:
            keyword_ids = self._validate_keywords(keywords)

        # Set max configuration file length
        if max_config_length is None:
            max_config_length = self._DEFAULT_MAX_CONFIG

        # Compute version string
        v_fields, version_string = self._compute_version_string(name, version)

        # If `relative_path` not passed, automatically generate it
        if relative_path is None:
            relative_path = _relpath_from_name(name, version_string, version_suffix)

        # See if there is already and entry with this name/version combination
        if location_type in ["dataregistry", "dummy"]:
            previous, previous_relpath, replace_iteration = self._find_previous(
                name, version_string, version_suffix, owner, owner_type
            )

            # When replacing, need to find what we are replacing
            if caller_function == "replace":
                if previous is None:
                    raise ValueError(
                        f"Dataset '{name}' does not exist, or it not overwritable"
                    )
                relative_path = previous_relpath
                replace_iteration = replace_iteration + 1

            # When registering, a previous (non-overwritten) entry cannot exist
            if caller_function == "register":
                if previous is not None:
                    raise ValueError(
                        "There is already a dataset with combination name,"
                        "version_string, version_suffix, owner, owner_type"
                    )
                replace_iteration = 0
        else:
            replace_iteration = 0

        # When replacing, tag the old dataset as overwritten, and delete
        dataset_table = self._get_table_metadata("dataset")
        if caller_function == "replace":
            with self._engine.connect() as conn:
                update_stmt = (
                    update(dataset_table)
                    .where(dataset_table.c.dataset_id == previous)
                    .values(is_overwritten=True)
                )
                conn.execute(update_stmt)
                conn.commit()

            # Delete the old data
            self.delete(previous)

        # ---------------------
        # Now regsister dataset
        # ---------------------

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
        values["replace_iteration"] = replace_iteration

        # We tentatively start with an "invalid" dataset in the database. This
        # will be upgraded to valid if the data copying (if any) was successful.
        values["status"] = 0

        # Create a new row in the data registry database.
        with self._engine.connect() as conn:
            prim_key = add_table_row(conn, dataset_table, values, commit=True)

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
        else:
            dataset_organization = location_type
            num_files = 0
            total_size = 0
            ds_creation_date = None

        # Case where user is overwriting the dataset `creation_date`
        if creation_date:
            ds_creation_date = creation_date

        # Copy was successful
        with self._engine.connect() as conn:
            # Update the entry with dataset metadata
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

            # Add any keyword tags
            if len(keywords) > 0:
                keyword_table = self._get_table_metadata("dataset_keyword")
                for k_id in keyword_ids:
                    add_table_row(
                        conn,
                        keyword_table,
                        {"dataset_id": prim_key, "keyword_id": k_id},
                        commit=False,
                    )

            conn.commit()

        # Update the metadata of the replaced dataset
        if caller_function == "replace":
            with self._engine.connect() as conn:
                update_stmt = (
                    update(dataset_table)
                    .where(dataset_table.c.dataset_id == previous)
                    .values(
                        replace_date=datetime.now(),
                        replace_uid=self._uid,
                        replace_id=prim_key,
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
            # Stop if we don't have write permission to the root_dir
            if not self.root_dir_write_access:
                raise Exception(
                    f"Cannot copy data, no write access to {self._root_dir}"
                )

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

    def _find_previous(self, name, version_string, version_suffix, owner, owner_type):
        """
        Find all dataset entries with the same `name`, `version`,
        `version_suffix`, `owner` and `owner_type`.

        We want to know, of those datasets, which are overwritable but have not
        yet been marked as overwritten. There should only be one, the latest
        one.

        Parameters
        ----------
        name/version/version_suffix/owner/owner_type : str

        Returns
        -------
        dataset_id : int
            The dataset ID of the latest entry with name, version etc
            combination. If no dataset with this combination is found, return
            None.
        relative_path : str
            The `relative_path` of the discovered dataset
        replace_iteration : int
            The number of times the dataset has been overwritten
        """

        # Search for dataset in the registry.
        dataset_table = self._get_table_metadata("dataset")
        stmt = select(
            dataset_table.c.dataset_id,
            dataset_table.c.is_overwritable,
            dataset_table.c.is_overwritten,
            dataset_table.c.relative_path,
            dataset_table.c.replace_iteration,
        )

        stmt = stmt.where(
            dataset_table.c.name == name,
            dataset_table.c.version_string == version_string,
            dataset_table.c.version_suffix == version_suffix,
            dataset_table.c.owner == owner,
            dataset_table.c.owner_type == owner_type,
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # Pull out the single result
        dataset_id = None
        relative_path = None
        replace_iteration = None
        for r in result:
            if not r.is_overwritten:
                if dataset_id is not None:
                    raise ValueError("Found more than one entry")
                dataset_id = r.dataset_id
                relative_path = r.relative_path
                replace_iteration = r.replace_iteration

        return dataset_id, relative_path, replace_iteration

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

    def _validate_keywords(self, keywords):
        """
        Validate a list of keywords.

            - Ensure they are strings
            - Ensure the chosen keywords are registered in the keywords table

        If any keyword is invalid an exception is raised.

        Parameters
        ----------
        keywords : list[str]

        Returns
        -------
        keyword_ids : list[int]
            The associated `keyword_id`s from the `keyword` table
        """

        keyword_ids = []

        for k in keywords:
            # Make sure keyword is a string
            if type(k) != str:
                raise ValueError(f"{k} is not a valid keyword string")

        # Make sure keywords are all in the keywords table
        keyword_table = self._get_table_metadata("keyword")

        stmt = select(keyword_table.c.keyword_id).where(
            keyword_table.c.keyword.in_(keywords)
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # Keyword found
        for r in result:
            keyword_ids.append(r.keyword_id)

        # Keyword not found
        if len(keyword_ids) != len(keywords):
            raise ValueError(f"Not all keywords selected are registered")

        return keyword_ids

    def add_keywords(self, dataset_id, keywords):
        """
        Add/append keywords to an already existing dataset.

        First check the keywords are valid, then append. If the dataset already
        has one or more of the passed keywords attributed to it, the keyword(s)
        will not be duplicated.

        Parameters
        ----------
        dataset_id : int
        keywords : list[str]
        """

        # Make sure things are valid
        if type(keywords) != list:
            raise ValueError("Passed keywords object must be a list")

        for k in keywords:
            if type(k) != str:
                raise ValueError(f"Keyword {k} is not a valid string")

        if len(keywords) == 0:
            return

        # Validate keywords (make sure they are in the `keyword` table)
        keyword_ids = self._validate_keywords(keywords)

        # Link fo the dataset-keyword association table
        dataset_keyword_table = self._get_table_metadata("dataset_keyword")

        with self._engine.connect() as conn:
            # Loop over each keyword in the list
            for keyword_id in keyword_ids:
                # Check if this dataset already has this keyword
                stmt = (
                    select(dataset_keyword_table)
                    .where(dataset_keyword_table.c.dataset_id == dataset_id)
                    .where(dataset_keyword_table.c.keyword_id == keyword_id)
                )

                result = conn.execute(stmt)

                # If we don't have the keyword, add it
                if result.rowcount == 0:
                    add_table_row(
                        conn,
                        dataset_keyword_table,
                        {"dataset_id": dataset_id, "keyword_id": keyword_id},
                        commit=False,
                    )

            conn.commit()

    def delete_keywords(self, dataset_id, keywords):
        """
        Remove keywords from a dataset.

        Parameters
        ----------
        dataset_id : int
        keywords : list[str]
        """

        raise NotImplementedError()
