import os
from dataregistry import DataRegistry


def dregs_show(show_what, args):
    """
    Calls helper functions from database to show table properties, quantities, etc

    Parameters
    ----------
    show_what : str
        What property are we showing? (["keywords"])
    args : argparse object

    args.config_file : str
        Path to data registry config file
    args.schema : str
        Which schema to search
    args.root_dir : str
        Path to root_dir
    args.site : str
        Look up root_dir using a site
    args.namespace : str
        Namespace to connect to
    args.query_mode : str
        Query mode type ("production" or "working"), used to select schema
    """

    # Establish connection to the regular schema
    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
        namespace=args.namespace,
        query_mode=args.query_mode,
    )

    if show_what == "keywords":
        print(f"Avaliable keywords:")
        print(datareg.Query.get_keyword_list())
