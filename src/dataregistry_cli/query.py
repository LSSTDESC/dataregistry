import os
from dataregistry import DataRegistry


def dregs_ls(args):
    """
    Queries the data registry for datasets, displaying their relative paths.

    Can apply a "owner" and/or "owner_type" filter.

    Note that the production schema will always be searched against, even if it
    is not the passed `schema`.

    Parameters
    ----------
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
    if datareg.db_connection.schema != "production":
        datareg_prod = DataRegistry(
            config_file=args.config_file,
            schema="production",
            root_dir=args.root_dir,
            site=args.site,
        )
    else:
        datareg_prod = None

    # Filter on dataset owner and/or owner_type
    filters = []

    print("\nDataRegistry query:", end=" ")
    if not args.all:
        if args.owner_type is not None:
            filters.append(Filter("dataset.owner_type", "==", args.owner_type))
            print(f"owner_type=={args.owner_type}", end=" ")

        if args.owner is None:
            if args.owner_type is None:
                filters.append(
                    datareg.Query.gen_filter("dataset.owner", "==", os.getenv("USER"))
                )
                print(f"owner=={os.getenv('USER')}", end=" ")
        else:
            filters.append(datareg.Query.gen_filter("dataset.owner", "==", args.owner))
            print(f"owner=={args.owner}", end=" ")
    else:
        print("all datasets", end=" ")

    # Loop over this schema and the production schema and print the results
    for this_datareg in [datareg, datareg_prod]:
        if this_datareg is None:
            continue
    
        mystr = f"Schema = {this_datareg.db_connection.schema}"
        print(f"\n{mystr}")
        print("-" * len(mystr))

        # Query
        results = this_datareg.Query.find_datasets(
            [
                "dataset.name",
                "dataset.version_string",
                "dataset.relative_path",
                "dataset.owner",
                "dataset.owner_type",
                "dataset.status",
            ],
            filters,
            return_format="dataframe",
        )

        print(results.to_string(index=False))
