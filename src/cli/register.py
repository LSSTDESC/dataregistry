import os
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

_lookup = {
    "production": ownertypeenum.production,
    "group": ownertypeenum.group,
    "user": ownertypeenum.user,
}


def _get_config_file(config_file):
    """
    Get configuration file to connect to database.

    If not passed manually, use default location.

    Parameters
    ----------
    config_file : str
    
    Returns
    -------
    - : str
        Path to config file
    """

    # Case where config file is manually passed
    if config_file:
        return config_file
    # Case where config file is stored an env variable
    elif os.getenv("DREGS_CONFIG") is not None:
        return os.getenv("DREGS_CONFIG")
    # Default location
    else:
        return os.path.join(os.getenv("HOME"), ".config_reg_writer")


def register_dataset(args):
    """
    Register a dataset in the DESC data registry.

    Parameters
    ----------
    args : argparse object
    """

    # Connect to database.
    engine, dialect = create_db_engine(config_file=_get_config_file(args.config_file))

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
