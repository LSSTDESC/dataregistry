from dataregistry.db_basic import DbConnection
from dataregistry.query import Query
from dataregistry.registrar import Registrar
import yaml
import os

_HERE = os.path.dirname(__file__)
_SITE_CONFIG_PATH = os.path.join(_HERE, "site_config", "site_rootdir.yaml")


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

        # Work out the location of the root directory
        root_dir = self._get_root_dir(root_dir)

        # Establish connection to database
        self.db_connection = DbConnection(config_file, schema=schema, verbose=verbose)

        # Create query object
        self.Query = Query(self.db_connection)

        # Create registrar object
        self.Registrar = Registrar(
            db_connection,
            root_dir,
            owner=owner,
            owner_type=owner_type,
        )

        # Create query object
        self.Query = Query(db_connection, root_dir)

    def _get_root_dir(self, root_dir):
        """
        What is the location of the root_dir we are pairing with?

        Either this is a predefined "site", such as "NERSC", or a manually
        passed path to the desired root directory. If `root_dir` is None then
        "NERSC" is used as the default.

        Parameters
        ----------
        root_dir : str

        Returns
        -------
        - : str
            Path to root directory
        """

        # Load the site config yaml file
        with open(_SITE_CONFIG_PATH) as f:
            data = yaml.safe_load(f)

        if root_dir is None:
            return data["nersc"]
        elif root_dir.lower() in data.keys():
            return data[root_dir.lower()]
        else:
            return root_dir
