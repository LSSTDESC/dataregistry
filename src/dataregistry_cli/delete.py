from datetime import datetime
import os
from dataregistry import DataRegistry


def delete_dataset(args):
    """
    Delete a dataset in the DESC data registry.

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

    args.dataset_id: int
        The dataset_id of the dataset we are deleting
    """

    # Connect to database.
    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
    )

    datareg.Registrar.dataset.delete(args.dataset_id)
