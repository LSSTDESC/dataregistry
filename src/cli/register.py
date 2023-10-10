import os
from dataregistry import DataRegistry


def register_dataset(args):
    """
    Register a dataset in the DESC data registry.

    Parameters
    ----------
    args : argparse object
    """

    # Connect to database.
    datareg = DataRegistry(config_file=args.config_file, schema=args.schema)

    # Register new dataset.
    new_id = datareg.Registrar.register_dataset(
        args.relative_path,
        args.version,
        name=args.name,
        version_suffix=args.version_suffix,
        creation_date=args.creation_date,
        description=args.description,
        old_location=args.old_location,
        copy=(not args.make_symlink),
        is_dummy=args.is_dummy,
        owner=args.owner
    )

    print(f"Created dataset entry with id {new_id}")
