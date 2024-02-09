import os

from dataregistry.db_basic import TableMetadata

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

        # Link to Table Metadata.
        self._metadata_getter = TableMetadata(db_connection)

        # Store user id
        self._uid = os.getenv("USER")

        # Default owner and owner_type's
        self._owner = owner
        self._owner_type = owner_type

        # Allowed owner types
        self._OWNER_TYPES = _OWNER_TYPES

        # Max configuration file length allowed
        self._DEFAULT_MAX_CONFIG = _DEFAULT_MAX_CONFIG

    def _get_table_metadata(self, tbl):
        return self._metadata_getter.get(tbl)

    def delete(self, entry_id):
        """
        Delete an entry from the DESC data registry.

        Parameters
        ----------
        entry_id : int
            The dataset/execution/etc ID we wish to delete from the database
        """

        raise NotImplementedError

        """
        Delete a dataset entry from the DESC data registry.

        This will remove the raw data from the root dir, but the dataset entry
        remains in the registry (now with `status=3`).

        Parameters
        ----------
        dataset_id : int
            Dataset we want to delete from the registry
        """

#        # First make sure the given dataset id is in the registry
#        dataset_table = self.parent._get_table_metadata("dataset")
#        previous_dataset = self._find_previous(dataset_table, dataset_id=dataset_id)
#
#        if previous_dataset is None:
#            raise ValueError(f"Dataset ID {dataset_id} does not exist")
#        if previous_dataset.status not in [0, 1]:
#            raise ValueError(f"Dataset ID {dataset_id} does not have a valid status")
#
#        # Update the status of the dataset to deleted
#        with self.parent._engine.connect() as conn:
#            update_stmt = (
#                update(dataset_table)
#                .where(dataset_table.c.dataset_id == dataset_id)
#                .values(
#                    status=3,
#                    delete_date=datetime.now(),
#                    delete_uid=self.parent._uid,
#                )
#            )
#            conn.execute(update_stmt)
#            conn.commit()
#
#        # Delete the physical data in the root_dir
#        if previous_dataset.status == 1:
#            data_path = _form_dataset_path(
#                previous_dataset.owner_type,
#                previous_dataset.owner,
#                previous_dataset.relative_path,
#                schema=self.parent._schema,
#                root_dir=self.parent._root_dir,
#            )
#            print(f"Deleting data {data_path}")
#            os.remove(data_path)
#
#        print(f"Deleted {dataset_id} from data registry")

    def modify(self, entry_id, modify_fields):
        """
        Modify an entry in the DESC data registry.

        Parameters
        ----------
        entry_id : int
            The dataset/execution/etc ID we wish to delete from the database
        modify_fields : dict
            Dict where key is the column to modify (must be allowed to modify)
            and value is the desired new value for the entry
        """

        raise NotImplementedError
