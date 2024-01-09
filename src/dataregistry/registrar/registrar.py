import os

from dataregistry.db_basic import TableMetadata

from .dataset import RegistrarDataset
from .dataset_alias import RegistrarDatasetAlias
from .execution import RegistrarExecution

__all__ = ["Registrar"]

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

        # Class wrappers which are used to create/modify/delete entries
        self.dataset = RegistrarDataset(self)
        self.execution = RegistrarExecution(self)
        self.dataset_alias = RegistrarDatasetAlias(self)

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
