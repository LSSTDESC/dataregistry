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

    args.owner : str
        Owner to list dataset entries for
    args.owner_type : str
        Owner type to list dataset entries for
    args.all : bool
        True to show all datasets, no filters
    args.config_file : str
        Path to data registry config file
    args.schema : str
        Which schema to search
    args.root_dir : str
        Path to root_dir
    args.site : str
        Look up root_dir using a site
    """

    # Establish connection to the regular schema
    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
    )

    # Establish connection to the production schema
    if datareg.db_connection.schema != args.prod_schema:
        datareg_prod = DataRegistry(
            config_file=args.config_file,
            schema=args.prod_schema,
            root_dir=args.root_dir,
            site=args.site,
        )
    else:
        datareg_prod = None

    if show_what == "keywords":
        print(f"Avaliable keywords in schema={datareg.db_connection.schema}:")
        print(datareg.Registrar.dataset.get_keywords())

        print(f"\nAvaliable keywords in schema={datareg_prod.db_connection.schema}:")
        print(datareg_prod.Registrar.dataset.get_keywords())
