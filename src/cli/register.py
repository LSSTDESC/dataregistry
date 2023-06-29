import os
import sys
import argparse
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


def make_entry(args):
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

parser = argparse.ArgumentParser(
    description="The DREGS CLI interface",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
subparsers = parser.add_subparsers(title="subcommand", dest="subcommand")

# Register a dataset.
arg_register = subparsers.add_parser("register", help="Register a dataset")

arg_register.add_argument(
    "relative_path",
    help="Destination for the dataset within the data registry. Path is relative to <registry root>/<owner_type>/<owner>.",
    type=str,
)
arg_register.add_argument(
    "version",
    help="Semantic version string of the format MAJOR.MINOR.PATCH or a special flag “patch”, “minor” or “major”. When a special flag is used it automatically bumps the relative version for you (see examples for more details).",
    type=str,
)
arg_register.add_argument(
    "--version_suffix",
    help="Optional suffix string to place at the end of the version string. Cannot be used for production datasets.",
    type=str,
)
arg_register.add_argument(
    "--name",
    help="Any convenient, evocative name for the human. Note the combination of name, version and version_suffix must be unique. If None name is generated from the relative path.",
    type=str,
)
arg_register.add_argument(
    "--creation_date", help="Manually set creation date of dataset"
)
arg_register.add_argument(
    "--description", help="Human-readable description of dataset", type=str
)
arg_register.add_argument(
    "--execution_id",
    help="Used to associate dataset with a particular execution",
    type=int,
)
arg_register.add_argument(
    "--access_API", help="Hint as to how to read the data", type=str
)

arg_register.add_argument(
    "--is_overwritable",
    help="True if dataset may be overwritten (defaults to False). Production datasets cannot be overwritten.",
    action="store_true",
)
arg_register.add_argument(
    "--old_location",
    help="Absolute location of dataset to copy.\nIf None dataset should already be at correct relative_path.",
    type=str,
)
arg_register.add_argument(
    "--make_symlink",
    help="Flag to make symlink to data rather than copy any files.",
    action="store_true",
)
arg_register.add_argument(
    "--is_dummy",
    help="True for “dummy” datasets (no data is copied, for testing purposes only)",
    action="store_true",
)
arg_register.add_argument(
    "--schema-version",
    default=f"{SCHEMA_VERSION}",
    help="Which schema to connect to",
)
arg_register.add_argument(
    "--locale",
    help="Location where dataset was produced",
    type=str,
    default="NERSC",
)
arg_register.add_argument("--owner", help="Owner of dataset. Defaults to $USER.")
arg_register.add_argument(
    "--owner-type", choices=["production", "group", "user"], default="user"
)
arg_register.add_argument(
    "--config_file", help="Location of DREGS config file", type=str
)

def main():
    args = parser.parse_args()

    if args.subcommand == "register":
        make_entry(args)
