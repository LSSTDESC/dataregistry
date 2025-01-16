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
    args.namespace_default_schema : str
        Which schema to default to within the namespace
    args.namespace : str
        Which namespace to connect to

    args.dataset_id: int
        The dataset_id of the dataset we are deleting
    """

    # Connect to database.
    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
        namespace_default_schema=args.namespace_default_schema,
        namespace=args.namespace
    )

    # Deleting directly using the dataset ID
    if hasattr(args, "dataset_id"):
        datareg.Registrar.dataset._delete_by_id(args.dataset_id, confirm=True)

    # Deleting based on name/version/owner/owner_type
    else:
        datareg.Registrar.dataset.delete(
            args.name, args.version_string, args.owner, args.owner_type, confirm=True
        )
