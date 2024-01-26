from .base_table_class import _OWNER_TYPES
from .dataset import DatasetTable
from .dataset_alias import DatasetAliasTable
from .execution import ExecutionTable

__all__ = ["Registrar"]


class Registrar:
    def __init__(
        self,
        db_connection,
        root_dir,
        owner=None,
        owner_type=None,
    ):
        """
        Wrapper registrar class.

        This holds callable subclasses for each table (dataset, execution and
        dataset_alias) which are used to register/modify/delete entries in
        those tables.

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

        # Class wrappers which are used to register/modify/delete entries in
        # the database
        self.execution = ExecutionTable(db_connection, root_dir, owner, owner_type)
        self.dataset_alias = DatasetAliasTable(
            db_connection, root_dir, owner, owner_type
        )
        self.dataset = DatasetTable(
            db_connection, root_dir, owner, owner_type, self.execution
        )

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
