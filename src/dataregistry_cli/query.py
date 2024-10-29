import os
from dataregistry import DataRegistry
import pandas as pd
from dataregistry import Filter


def _render_filters(datareg, args):
    """
    Apply a filter to the query.

    Both the owner and owner_type columns can be filtered against. In addition,
    the dataset name column can be filtered against (allowed for % wildcards).

    Keywords can also be filtered against. These have to be treated separately,
    as they need to be linked to the keywords table.

    Parameters
    ----------
    datareg : DataRegistry object
    args : argparse object

    Returns
    -------
    filters : list[Filter]
    """

    filters = []

    # Dataset columns we can filter by
    queriables = ["owner", "owner_type", "name"]

    print("\nDataRegistry query:", end=" ")
    for col in queriables:
        # Add filter on this column
        if getattr(args, col) is not None:
            if col == "name":
                filters.append(Filter(f"dataset.{col}", "~=", getattr(args, col)))
            else:
                if not (col == "owner" and getattr(args, col).lower() == "none"):
                    filters.append(Filter(f"dataset.{col}", "==", getattr(args, col)))
            print(f"{col}=={getattr(args, col)}", end=" ")

    # Add keywords filter
    if args.keyword is not None:
        filters.append(datareg.Query.gen_filter("keyword.keyword", "==", args.keyword))

    return filters


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
    args.name : str
        Filter to only those results with a given dataset name (% can be used
        as a wildcard)
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
    args.return_cols : list[str]
        List of dataset columns to return
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
    if datareg.db_connection.schema != args.prod_schema:
        datareg_prod = DataRegistry(
            config_file=args.config_file,
            schema=args.prod_schema,
            root_dir=args.root_dir,
            site=args.site,
        )
    else:
        datareg_prod = None

    # By default, search for "our" dataset
    if args.owner is None:
        args.owner = os.getenv("USER")

    # Render search filters
    filters = _render_filters(datareg, args)

    # What columns are we printing
    _print_cols = [
        "dataset.name",
        "dataset.version_string",
        "dataset.owner",
        "dataset.owner_type",
        "dataset.description",
    ]
    if args.return_cols is not None:
        _print_cols = [f"dataset.{x}" for x in args.return_cols]
    if args.keyword is not None:
        _print_cols.append("keyword.keyword")

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
