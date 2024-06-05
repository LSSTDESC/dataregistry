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

        The DataRegistry class links to both the Registrar class, to
        register/modify/delete datasets, and the Query class, to query existing
        datasets.

        Links to the database is done automatically using the:
            - the users config file (if None defaults are used)
            - the passed schema (if None the default schema is used)

        The `root_dir` is the location the data is copied to. This can be
        manually passed, or alternately a predefined `site` can be chosen. If
        nether are chosen, the NERSC site will be selected as the default.

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
        self.db_connection = DbConnection(config_file, schema=schema,
                                          verbose=verbose)

        # Work out the location of the root directory
        self.root_dir = self._get_root_dir(root_dir, site)

        # Create registrar object
        self.Registrar = Registrar(self.db_connection, self.root_dir,
                                   owner, owner_type)

        # Create query object
        self.Query = Query(self.db_connection, self.root_dir)

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

        # Load the site config yaml file
        with open(_SITE_CONFIG_PATH) as f:
            data = yaml.safe_load(f)

        # Sqlite case
        if self.db_connection._dialect == "sqlite":
            # Sqlite cannot work with `site`s, must pass a `root_dir`
            if root_dir is None:
                raise ValueError("Must pass a `root_dir` using Sqlite")
            else:
                # root_dir cannot equal a site path when using Sqlite
                for a, v in data.items():
                    if root_dir == v:
                        raise ValueError(
                            "`root_dir` must not equal a pre-defined site with Sqlite"
                        )
            return root_dir

        # Non Sqlite case
        else:
            if root_dir is None:
                if site is not None:
                    if site.lower() not in data.keys():
                        raise ValueError(f"{site} is not a valid site")
                    root_dir = data[site.lower()]
                elif os.getenv("DATAREG_SITE"):
                    root_dir = data[os.getenv("DATAREG_SITE").lower()]
                else:
                    root_dir = data["nersc"]

            return root_dir
