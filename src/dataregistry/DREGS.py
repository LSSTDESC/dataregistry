import os

from dataregistry.db_basic import SCHEMA_VERSION, DbConnection
from dataregistry.query import Query
from dataregistry.registrar import Registrar


class DREGS:
    def __init__(
        self,
        owner=None,
        owner_type=None,
        config_file=None,
        schema_version=None,
        root_dir=None,
        verbose=False,
    ):
        """
        Primary data registry (DREGS) wrapper class.

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
        schema_version : str
            Schema to connect to, if None, default schema is assumed.
        root_dir : str
            Root directory for DREGS datasets, if None, default is assumed.
        verbose : bool
            True for more output.
        """

        # If no schema specified, go to the default one
        if schema_version is None:
            schema_version = SCHEMA_VERSION

        # Establish connection to database
        db_connection = DbConnection(config_file,
                                     schema=schema_version,
                                     verbose=verbose)
        #engine, dialect = create_db_engine(config_file=config_file, verbose=verbose)

        # Create query object
        self.Query = Query(db_connection)

        # Create registrar object
        self.Registrar = Registrar(
            db_connection,
            owner=owner,
            owner_type=owner_type,
            root_dir=root_dir,
        )
