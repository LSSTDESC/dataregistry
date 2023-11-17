from dataregistry.db_basic import DbConnection
from dataregistry.query import Query
from dataregistry.registrar import Registrar


class DataRegistry:
    def __init__(
        self,
        owner=None,
        owner_type=None,
        config_file=None,
        schema=None,
        root_dir=None,
        verbose=False,
    ):
        """
        Primary data registry wrapper class.

        Class links to both the Registrar class, to registry new dataset, and
        the Query class, to query existing datasets.

        Links to the database is done automatically using the default
        assumptions (i.e., the users config file is in the default location and
        the schema to connect to and the root directory are the defaults.
        However each can also be manually specified.

        Parameters
        ----------
        owner : str
            To set the default owner for all registered datasets in this
            instance.
        owner_type : str
            To set the default owner_type for all registered datasets in this
            instance.
        config_file : str
            Path to config file, if None, default location is assumed.
        schema : str
            Schema to connect to, if None, default schema is assumed.
        root_dir : str
            Root directory for datasets, if None, default is assumed.
        verbose : bool
            True for more output.
        """
        # Establish connection to database
        self.db_connection = DbConnection(config_file, schema=schema, verbose=verbose)

        # Create query object
        self.Query = Query(self.db_connection)

        # Create registrar object
        self.Registrar = Registrar(
            self.db_connection,
            owner=owner,
            owner_type=owner_type,
            root_dir=root_dir,
        )

        # Create query object
        self.Query = Query(self.db_connection, root_dir=self.Registrar.root_dir)

    @property
    def root_dir(self):
        return self.Registrar.root_dir
