from dataregistry.db_basic import DbConnection
from dataregistry.query import Query
from dataregistry.registrar import Registrar
from dataregistry.registrar.registrar_util import _form_dataset_path
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

    def easy_query(self, return_format="list_of_dicts", **query):
        """
        Run a query on the registry with a simple syntax. For example, you can do:

        # everything belonging to a specific owner
        results = registry.easy_query(owner="jbogart")

        # a specific dataset
        results = registry.easy_query(dataset_id=30)

        # you can combine search terms, which are ANDed together:
        results = registry.easy_query(owner="jbogart", version_major=2)

        Once issue 220 in the data registry is resolved then you will also
        be able to do searches with operators:

        results = registry.easy_query(total_disk_size_gt=100)

        Current supported search terms are:
            access_api
            access_api_configuration
            archive_date
            archive_path
            contact_email
            creation_date
            creator_uid
            data_org
            dataset_id
            delete_date
            delete_uid
            description
            execution_id
            is_overwritable
            location_type
            move_date
            name
            nfiles
            owner
            owner_type
            register_date
            register_root_dir
            relative_path
            replace_id
            replace_iteration
            status
            total_disk_space
            url
            version_major
            version_minor
            version_patch
            version_string
            path

        Parameters
        ----------

        return_format : str
            The format to return the results in. Options are "list_of_dicts",
            "dataframe", "dict_of_lists". The default is "list_of_dicts".
            "dict_of_lists" matches the format return by find_datasets.

        **query : dict
            The query parameters. Currently these should always be
            of the form field=value, where field is one of the search
            terms listed above.

        Returns
        -------
        list of dict
            A list of datasets matching the query, where each dataset is represented as a dict of its
            metadata fields. The keys of the dicts will be the same as the search terms listed above.
            Note that internally the keys are prefixed with "dataset.", but this function will remove
            that prefix for convenience.


        """
        filters = []
        ops = {
            "_lt": "<",
            "_le": "<=",
            "_gt": ">",
            "_ge": ">=",
            "_ne": "!="
        }
        # parse the query keywords and turn them all into filters.
        for k, v in query.items():
            # check for queries that end with a search operator, e.g. total_disk_size_gt=100.
            # This approach is taken from sqlalchemy.
            for suffix, op in ops.items():
                # Check if the search term is a search operator
                if k.endswith(suffix):
                    # This is currently broken so only support _ne,
                    # but remove this when issue 220 in the data registry is resolved.
                    if suffix != "_ne":
                        raise ValueError("Querying with _gt, _ge, _lt, or _le is not currently supported due to a dataregistry issue 220.")
                    base_key = k[:-len(suffix)]
                    # Generate that actual query object
                    f = self.query.gen_filter("dataset." + base_key, op, v)
                    filters.append(f)
                    break
            # If none of the suffixes matched, then this is a normal query of the form field=value.
            else:
                f = self.query.gen_filter("dataset." + k, "==", v)
                filters.append(f)

        # run the actual query and ask for a dataframe back for convenience.
        results = self.query.find_datasets(filters=filters, return_format='dataframe')


        # We will need this schema information to
        # generate the absolute path for each dataset.
        if self.db_connection._query_mode == "both":
            schema = "working"
        else:
            schema = self.db_connection._query_mode
        if not self.db_connection._namespace:
            schema_name = None
        else:
            schema_name = self.db_connection._namespace + '_' + schema

        # Get the absolute path for each dataset.
        # We avoid using the query.get_dataset_absolute_path function here
        # because we have already queried all the info we need to
        # generate the path and that would require another DB
        # query per result.
        def _path_from_row(row):
            owner_type = row["dataset.owner_type"]
            if owner_type is None or owner_type != owner_type:
                return None
            return _form_dataset_path(
                owner_type,
                row["dataset.owner"],
                row["dataset.relative_path"],
                schema=schema_name,
                root_dir=self.root_dir,
            )

        results["dataset.path"] = results.apply(_path_from_row, axis=1)

        # remove the "dataset." prefix from the keys
        results = results.rename(
            columns=lambda key: key[len("dataset."):] if key.startswith("dataset.") else key
        )

        if return_format == "dataframe":
            return results
        elif return_format == "dict_of_lists":
            return results.to_dict(orient='list')
        elif return_format == "list_of_dicts":
            return results.to_dict(orient='records')

        raise ValueError(f"Invalid return_format {return_format}")

    # Simplify calls to functions in Registrar object
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

    def register_dataset(
            self,
            name,
            version,
            **kw,
            ):
        """
        Convenience function which just calls
        DataRegistry.registrar.dataset.register.   See DatasetTable.register
        for complete argument and return description.
        """
        return self.registrar.dataset.register(name, version, **kw)

    def replace_dataset(
            self,
            name,
            version,
            **kw,
            ):
        """
        Convenience function which just calls
        DataRegistry.registrar.dataset.replace.   See DatasetTable.replace
        for complete argument and return description.
        """
        return self.registrar.dataset.replace(name, version, **kw)

    def delete_dataset(
            self,
            name,
            version_string,
            owner,
            owner_type,
            confirm=False):
        """
        Convenience function which just calls
        DataRegistry.registrar.dataset.delete.   See DatasetTable.delete
        for complete argument and return description.
        """
        return self.registrar.dataset.delete(name, version_string, owner,
                                             owner_type, confirm=confirm)

    def add_keywords_to_dataset(self, dataset_id, keyword):
        """
        Add keywords to a dataset entry.

        Parameters
        ----------
        dataset_id : int
            Dataset id to add keyword to
        keyword : list[str]
            Keywords to add to dataset
        """
        return self.registrar.keyword.add_keywords_to_dataset(dataset_id,
                                                              keyword)

    def remove_keywords_froom_dataset(self, dataset_id, keyword):
        """
        Remove keywords from a dataset entry.

        Parameters
        ----------
        dataset_id : int
            Dataset id to remove keyword from
        keyword : list[str]
            Keywords to remove from dataset
        """
        return self.registrar.keyword.remove_keywords_from_dataset(dataset_id,
                                                                   keyword)

    def create_keywords(self, keywords, user_type="user", system=False,
                        commit=True):
        """
        Keyword.create_keywords for complete description of arguments
        """
        return self.registrar.keyword.create_keywords(keywords, )

    # Simplify calls to functions in Query object
    def find_datasets(self, **kwargs):
        """
        Convenience function which just calls the find_datasets function
        of the Query object. See full documentation there.
        """
        return self.query.find_datasets(**kwargs)

    def get_dataset_absolute_path(self, dataset_id, schema=None, silent=True):
        """
        See Query.get_dataset_absolute_path for complete description.
        """
        return self.query.get_dataset_absolute_path(dataset_id, schema=schema,
                                                    silent=silent)

    def get_all_tables(self):
        """
        See Query.get_all_tables for complete description)
        """
        return self.query.get_all_tables()

    def get_all_columns(self, table="dataset", include_table=True,
                        include_schema=False):
        """
        See Query.get_all_columns for complete description
        """
        return self.query.get_all_columns(table=table,
                                          include_table=include_table,
                                          include_schema=include_schema)

    def get_keyword_list(self, query_mode=None):
        """
        See Query.get_keyword_list for complete description
        """
        return self.query.get_keyword_list(query_mode=query_mode)

    # Register execution
    def register_execution(self, name, **kwargs):
        """
        See full documentation under ExecutionTable.register
        """
        return self.registrar.execution.register(name, **kwargs)
