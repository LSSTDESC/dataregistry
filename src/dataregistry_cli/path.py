from dataregistry import DataRegistry


def dregs_path(args):
    """
    Print the absolute path for one dataset.

    Parameters
    ----------
    args : argparse object

    args.dataset_id : int
        Dataset id to resolve
    args.schema_mode : str or None
        Which schema mode (working/production) to query
    args.config_file : str
        Path to data registry config file
    args.schema : str
        Which schema to connect to
    args.root_dir : str
        Path to root_dir
    args.site : str
        Look up root_dir using a site
    args.namespace : str
        Namespace to connect to
    args.query_mode : str
        Which schema(s) to probe
    """

    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
        namespace=args.namespace,
        query_mode=args.query_mode,
    )

    dataset_path = datareg.Query.get_dataset_absolute_path(
        args.dataset_id, schema=args.schema_mode, silent=False
    )
    print(dataset_path)
