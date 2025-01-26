from datetime import datetime
import os
from dataregistry import DataRegistry


def modify_dataset(args):
    """
    Modify a dataset in the DESC data registry.

    Note only certain columns are allowed to be modified.

    Through the CLI we can only modify one column to a new value at a time.

    Parameters
    ----------
    args : argparse object

    args.config_file : str
        Path to data registry config file
    args.schema : str
        Which schema to search
    args.root_dir : str
        Path to root_dir
    args.site : str
        Look up root_dir using a site
    args.dataset_id : int
        The dataset to modify
    args.column : str
        The column in the dataset table we are modifying
    args.value : str
        The updated value
    args.entry_mode : str
        Which schema to default to in the namespace
    args.namespace : str
        Which namespace to connect to
    """

    # Connect to database.
    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
        entry_mode=args.entry_mode,
        namespace=args.namespace,
    )

    # Modify dataset.
    datareg.Registrar.dataset.modify(args.dataset_id, {args.column: args.new_value})

    print(
        f"Modified dataset {args.dataset_id} column {args.column} to '{args.new_value}'"
    )
