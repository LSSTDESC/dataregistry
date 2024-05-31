from datetime import datetime
import os
from dataregistry import DataRegistry


def register_dataset(args):
    """
    Register a dataset in the DESC data registry.

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

    Information about the arguments that go into `register_dataset` can be
    found in `src/cli/cli.py` or by running `dregs --help`.
    """

    # Convert to a datetime object (needed for SQLite)
    if args.creation_date is not None:
        args.creation_date = datetime.strptime(args.creation_date, "%Y-%m-%d")

    # Connect to database.
    datareg = DataRegistry(
        config_file=args.config_file,
        schema=args.schema,
        root_dir=args.root_dir,
        site=args.site,
    )

    # Register new dataset.
    new_id = datareg.Registrar.dataset.register(
        args.relative_path,
        args.version,
        name=args.name,
        version_suffix=args.version_suffix,
        creation_date=args.creation_date,
        access_api=args.access_api,
        execution_id=args.execution_id,
        is_overwritable=args.is_overwritable,
        description=args.description,
        old_location=args.old_location,
        copy=(not args.make_symlink),
        owner=args.owner,
        owner_type=args.owner_type,
        execution_name=args.execution_name,
        execution_description=args.execution_description,
        execution_start=args.execution_start,
        execution_site=args.execution_site,
        execution_configuration=args.execution_configuration,
        input_datasets=args.input_datasets,
        location_type=args.location_type,
        url=args.url,
        contact_email=args.contact_email,
    )

    print(f"Created dataset entry with id {new_id}")
