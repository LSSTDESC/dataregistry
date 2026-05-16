from dataregistry.db_basic import DbConnection
from dataregistry.query import Query
from dataregistry.registrar import Registrar
import yaml
import os
import logging

_HERE = os.path.dirname(__file__)
_SITE_CONFIG_PATH = os.path.join(_HERE, "site_config", "site_rootdir.yaml")


class DataRegistry:
    def __init__(
        self,
        owner=None,
        owner_type=None,
        config_file=None,
        root_dir=None,
        logging_level=logging.INFO,
        site=None,
        namespace=None,
        schema=None,
        entry_mode="working",
        query_mode="both",
    ):
        """
        Primary data registry wrapper class.

        Each DataRegistry instance has as members an instance of the Registrar
        class, to register/modify/delete datasets, and of the Query class,
        to query existing datasets.

        Access to the database is handled automatically using:
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
            instance, one of "production", "project", "group", "user".
            Defaults to "user".
        config_file : str
            Path to config file, if None, use default NERSC config.
        root_dir : str
            Root directory for datasets, if None, default is assumed.
        logging_level : int, optional
            Level for the logger output (default is logging.INFO)
        site : str
            Can be used instead of `root_dir`. Some predefined "sites" are
            built in, such as "nersc", which will set the `root_dir` to the
            data registry's default data location at NERSC.
        namespace : str, optional
            Namespace to connect to. If None, the default namespace
            ("lsst_desc") will be used
        schema : str, optional
            Schema to connect to, to connect directly to a chosen schema,
            bypassing the namespace.
        entry_mode : str, optional
            Which schema ("working" or "production") within the namespace to
            use when writing/modifying/deleting entries.
        query_mode : str, optional
            Which schema(s) ("working" or "production") to probe when querying.
            By default query_mode="both", which searches both schemas together,
            however this can be restricted to either "working" or "production"
            to restrict searches to a single schema.
        """

        # Establish connection to database
        self.db_connection = DbConnection(
            config_file=config_file,
            schema=schema,
            logging_level=logging_level,
            namespace=namespace,
            entry_mode=entry_mode,
            query_mode=query_mode,
        )

        # Work out the location of the root directory
        self.root_dir = self._get_root_dir(root_dir, site)

        # Create registrar object
        self.registrar = Registrar(self.db_connection, self.root_dir, owner,
                                   owner_type)
        self.Registrar = self.registrar  # for backward compatibility

        # Create query object
        self.query = Query(self.db_connection, self.root_dir)
        self.Query = self.query  # for backward compatibility

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

    def fetch(self, dataset_id, schema_type="working",
              destination_path=None, destination_endpoint="NERSC DTN",
              no_cfs_copy=False):
        """
        Fetch a registered dataset. This is just a wrapper which calls
        Registrar.fetch, supply the Query object as an argument.

        Behavior depends on arguments and
        whether dataset is available in cfs or only from archive, but
        archiving is not yet implemented, so the only possibility is:

        * If destination_path is not None, copy from cfg to user-specified
          path, at user-specified globus endpoint.

        Parameters
        ----------
        dataset_id : int
            id of dataset to be retrieved
        schema_type : string
            one of "working" (the default) or "production"
        destination_path : string
            where to put the dataset.  If None, defaults to absolute
            path in cfs assigned to this dataset
        destination_endoint : string
            globus endpoint to which dataa will be written. Defaults to
            "NERSC DTN"
        no_cfs_copy : boolean
            If True and dataset was absent from cfs, write directly to
            to the destination requested; do not also restore to cfs.

        Returns
        -------
        Absolute cfs path of dataset when it was registered
        """
        return self.registrar.dataset.fetch(self.query, dataset_id,
                                            schema_type, destination_path,
                                            destination_endpoint, no_cfs_copy)

    def find_datasets(
            self,
            property_names=None,
            filters=[],
            return_format="property_dict",
            strip_table_names=False,
            schema_mode=None,
            ):
        """
        Convenience function which just calls the find_datasets function
        of the Query object
        """
        return self.query.find_datasets(
            property_names=property_names,
            filters=[],
            return_format=return_format,
            strip_table_names=strip_table_names,
            schema_mode=schema_mode,
            )
