import os
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

_lookup = {
    "production": ownertypeenum.production,
    "group": ownertypeenum.group,
    "user": ownertypeenum.user,
}


def register_dataset(args):
    """
    Register a dataset in the DESC data registry.

    Parameters
    ----------
    args : argparse object
    """

    # Connect to database.
    engine, dialect = create_db_engine(config_file=args.config_file)

    # Select schema
    if args.schema_version:
        schema = args.schema_version
    else:
        schema = SCHEMA_VERSION

    # Deduce owner of dataset
    owner = args.owner
    if args.owner_type == "production":
        owner = "production"

    if not owner:
        owner = os.getenv("USER")

    # Register dataset using passed arguments.
    registrar = Registrar(
        engine, dialect, _lookup[args.owner_type], owner=owner, schema_version=schema
    )

    new_id = registrar.register_dataset(
        args.relative_path,
        args.version,
        name=args.name,
        version_suffix=args.version_suffix,
        creation_date=args.creation_date,
        description=args.description,
        old_location=args.old_location,
        copy=(not args.make_symlink),
        is_dummy=args.is_dummy,
    )

    print(f"Created dataset entry with id {new_id}")
