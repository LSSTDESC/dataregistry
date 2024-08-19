import os
from dataregistry import DataRegistry
import pandas as pd


def dregs_ls(args):
    """
    Queries the data registry for datasets, displaying various attributes.

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
    args.extended : bool
        True to list more dataset properties
    args.max_chars : int
        Maximum number of character to print per column
    args.max_rows : int
        Maximum number of rows to print
    args.keywords : list[str]
        Search by an additional list of keywords
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
        # Add owner_type filter
        if args.owner_type is not None:
            filters.append(Filter("dataset.owner_type", "==", args.owner_type))
            print(f"owner_type=={args.owner_type}", end=" ")

        # Add owner filter
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

    # What columns are we printing
    _print_cols = [
        "dataset.name",
        "dataset.version_string",
        "dataset.owner",
        "dataset.owner_type",
        "dataset.description",
    ]
    if args.extended:
        _print_cols.extend(
            [
                "dataset.dataset_id",
                "dataset.relative_path",
                "dataset.status",
                "dataset.register_date",
                "dataset.is_overwritable",
            ]
        )

    # Add keywords filter
    if args.keyword is not None:
        _print_cols.append("keyword.keyword")

        filters.append(datareg.Query.gen_filter("keyword.keyword", "==", args.keyword))

    # Loop over this schema and the production schema and print the results
    for this_datareg in [datareg, datareg_prod]:
        if this_datareg is None:
            continue

        mystr = f"Schema = {this_datareg.db_connection.schema}"
        print(f"\n{mystr}")
        print("-" * len(mystr))

        # Query
        results = this_datareg.Query.find_datasets(
            [x for x in _print_cols],
            filters,
            return_format="dataframe",
        )

        # Strip "dataset." from column names
        new_col = {
            x: x.split("dataset.")[1] for x in results.columns if "dataset." in x
        }
        results.rename(columns=new_col, inplace=True)

        # Add compressed columns
        if "owner" in results.keys():
            results["type/owner"] = results["owner_type"] + "/" + results["owner"]
            del results["owner"]
            del results["owner_type"]

        if "register_date" in results.keys():
            results["register_date"] = results["register_date"].dt.date

        if "keyword.keyword" in results.keys():
            del results["keyword.keyword"]

        # Print
        with pd.option_context(
            "display.max_colwidth", args.max_chars, "display.max_rows", args.max_rows
        ):
            print(results)
