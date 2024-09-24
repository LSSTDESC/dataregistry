import os

from dataregistry.db_basic import TableMetadata
from dataregistry.schema import load_schema
from sqlalchemy import select, update
from datetime import datetime

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
from dataregistry.db_basic import add_table_row

# Allowed owner types
_OWNER_TYPES = {"user", "project", "group", "production"}

# Default maximum allowed length of configuration file allowed to be ingested
_DEFAULT_MAX_CONFIG = 10000


class BaseTable:
    def __init__(self, db_connection, root_dir, owner, owner_type):
        """
        Base class to register/modify/delete entries in the database tables.

        Each table subclass (e.g., DatasetTable) will inherit this class.

        Functions universal to all tables, such as delete and modify are
        written here, the register function, and other unique functions for the
        tables, are in their respective subclasses.

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
        self._dialect = db_connection._dialect

        # Link to Table Metadata.
        self._table_metadata = TableMetadata(db_connection)

        # Store user id
        self._uid = os.getenv("USER")

        # Default owner and owner_type's
        self._owner = owner
        self._owner_type = owner_type

        # Allowed owner types
        self._OWNER_TYPES = _OWNER_TYPES

        # Max configuration file length allowed
        self._DEFAULT_MAX_CONFIG = _DEFAULT_MAX_CONFIG

        # Load and store the schema yaml file
        self.schema_yaml = load_schema()

    def _get_table_metadata(self, tbl):
        return self._table_metadata.get(tbl)

    def delete(self, entry_id):
        """
        Delete an entry from the DESC data registry.

        Parameters
        ----------
        entry_id : int
            Entry we want to delete from the registry
        """

        raise NotImplementedError

    def modify(self, entry_id, modify_fields):
        """
        Modify an entry in the DESC data registry.

        Only certain columns are allowed to be modified after registration,
        this is defined in the schema yaml file.

        Parameters
        ----------
        entry_id : int
            The dataset/execution/etc ID we wish to delete from the database
        modify_fields : dict
            Dict where key is the column to modify (must be allowed to modify)
            and value is the desired new value for the entry
        """

        assert (
            type(modify_fields) == dict
        ), f"modify_fields is expected as a dict, {'column': new_values}"

        # First make sure the given entry is in the registry
        my_table = self._get_table_metadata(self.which_table)
        previous_entry = self.find_entry(entry_id, raise_if_not_found=True)

        # Loop over each column to be modified
        for key, v in modify_fields.items():
            # Make sure the column is in the schema
            if (
                key
                not in self.schema_yaml["tables"][self.which_table][
                    "column_definitions"
                ].keys()
            ):
                raise ValueError(f"The column {key} does not exist in the schema")

            # Make sure the column is modifiable
            if not self.schema_yaml["tables"][self.which_table]["column_definitions"][
                key
            ]["modifiable"]:
                raise ValueError(f"The column {key} is not modifiable")

        with self._engine.connect() as conn:
            # Update the metadata
            if len(modify_fields.keys()) > 0:
                update_stmt = (
                    update(my_table)
                    .where(getattr(my_table.c, self.entry_id) == entry_id)
                    .values(modify_fields)
                )
                conn.execute(update_stmt)

            conn.commit()

    def find_entry(self, entry_id, raise_if_not_found=False):
        """
        Find an entry in the database.

        Parameters
        ----------
        entry_id : int
            Unique identifier for table entry
            e.g., dataset_id for the dataset table
        raise_if_not_found : bool, optional
            Raise an exception if the entry is not found

        Returns
        -------
        r : CursorResult object
            Found entry (None if no entry found)
        """

        # Search for entry in the registry.
        my_table = self._get_table_metadata(self.which_table)
        stmt = select(my_table).where(getattr(my_table.c, self.entry_id) == entry_id)

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # Pull out the single result
        for r in result:
            return r

        # Raise an exception if we did not find the entry
        if raise_if_not_found:
            raise ValueError(f"Entry {entry_id} not found in {self.which_table}")

        # No results found
        return None

    def get_modifiable_columns(self):
        """
        Return a list of all columns in this table that are "modifiable".

        As defined in the schema yaml file.

        Returns
        -------
        mod_list : list[str]
        """

        mod_list = []
        for att in self.schema_yaml["tables"][self.which_table]["column_definitions"]:
            if self.schema_yaml["tables"][self.which_table]["column_definitions"][att][
                "modifiable"
            ]:
                mod_list.append(att)

        return mod_list

    def get_keywords(self):
        """
        Returns the list of system keywords that are allowed.

        Returns
        -------
        keywords : list[str]
        """

        keywords = []

        # Query for all keywords
        keyword_table = self._get_table_metadata("keyword")
        stmt = select(keyword_table.c.keyword)

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        for r in result:
            keywords.append(r.keyword)

        return keywords
