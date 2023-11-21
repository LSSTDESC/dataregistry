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
        site=None,
    ):
        """
        Primary data registry wrapper class.

        Class links to both the Registrar class, to registry new dataset, and
        the Query class, to query existing datasets.

        Links to the database is done automatically using the:
            - the users config file (if None defaults are used)
            - the passed schema (if None default is used)

        The `root_dir` is the location the data is copied to. This can be
        manually passed, or alternately a predefined `site` can be chosen. If
        nether are chosen, the NERSC site will be selected.

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
        site : str
            Can be used instead of `root_dir`. Some predefined "sites" are
            built in, such as "nersc", which will set the `root_dir` to the
            data registry's default data location at NERSC.
        """

        # Establish connection to database
        self.db_connection = DbConnection(config_file, schema=schema, verbose=verbose)

        # Work out the location of the root directory
        root_dir = self._get_root_dir(root_dir, site)

        # Create registrar object
        self.Registrar = Registrar(
            self.db_connection,
            root_dir,
            owner=owner,
            owner_type=owner_type,
        )

        # Create query object
        self.Query = Query(self.db_connection, root_dir)

    def _get_root_dir(self, root_dir, site):
        """
        What is the location of the root_dir we are pairing with?

        In order of priority:
            - If manually passed `root_dir` is not None, use that.
            - If manually passed `site` is not None, use that.
            - If env DATAREG_SITE is set, use that.
            - Else use `site="nersc"`.

        All `site`s are assumed to be postgres. Sqlite users must manually
        specify the `root_dir.

        Parameters
        ----------
        root_dir : str
        site : str

        Returns
        -------
        - : str
            Path to root directory
        """

        # Sqlite cannot work with `site`s
        if self.db_connection._dialect == "sqlite":
            if site is not None:
                raise ValueError("Site's not allowed with Sqlite")

        # Load the site config yaml file
        with open(_SITE_CONFIG_PATH) as f:
            data = yaml.safe_load(f)

        # Find out what the root_dir should be
        if root_dir is None:
            if site is not None:
                if site.lower() not in data.keys():
                    raise ValueError(f"{site} is not a valid site")
                root_dir = data[site.lower()]
            elif os.getenv("DATAREG_SITE"):
                root_dir = os.getenv("DATAREG_SITE")
            else:
                if self.db_connection._dialect == "sqlite":
                    raise ValueError(
                        "For Sqlite, must pass root_dir or set $DATAREG_SITE"
                    )
                else:
                    root_dir = data["nersc"]

        if self.db_connection._dialect == "sqlite":
            # root_dir cannot equal a site path when using Sqlite
            for a, v in data.items():
                if root_dir == v:
                    raise ValueError(
                        "Cannot have `root_dir` equal to a pre-defined site when using Sqlite"
                    )

        return root_dir
