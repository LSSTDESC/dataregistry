import os
import sys
import argparse
from dataregistry.db_basic import SCHEMA_VERSION
from .register import register_dataset

parser = argparse.ArgumentParser(
    description="The DREGS CLI interface",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
subparsers = parser.add_subparsers(title="subcommand", dest="subcommand")

# Register a new database entry.
arg_register = subparsers.add_parser("register", help="Register a new entry to the database")

arg_register_sub = arg_register.add_subparsers(
    title="register what?", dest="register_type"
)

# Register a new dataset.
arg_register_dataset = arg_register_sub.add_parser("dataset", help="Register a dataset")

arg_register_dataset.add_argument(
    "relative_path",
    help=(
        "Destination for the dataset within the data registry. Path is"
        "relative to <registry root>/<owner_type>/<owner>."
    ),
    type=str,
)
arg_register_dataset.add_argument(
    "version",
    help=(
        "Semantic version string of the format MAJOR.MINOR.PATCH or a special"
        "flag “patch”, “minor” or “major”. When a special flag is used it"
        "automatically bumps the relative version for you (see examples for more"
        "details)."
    ),
    type=str,
)
arg_register_dataset.add_argument(
    "--version_suffix",
    help=(
        "Optional suffix string to place at the end of the version string."
        "Cannot be used for production datasets."
    ),
    type=str,
)
arg_register_dataset.add_argument(
    "--name",
    help=(
        "Any convenient, evocative name for the human. Note the combination of"
        "name, version and version_suffix must be unique. If None name is generated"
        "from the relative path."
    ),
    type=str,
)
arg_register_dataset.add_argument(
    "--creation_date", help="Manually set creation date of dataset"
)
arg_register_dataset.add_argument(
    "--description", help="Human-readable description of dataset", type=str
)
arg_register_dataset.add_argument(
    "--execution_id",
    help="Used to associate dataset with a particular execution",
    type=int,
)
arg_register_dataset.add_argument(
    "--access_API", help="Hint as to how to read the data", type=str
)
arg_register_dataset.add_argument(
    "--is_overwritable",
    help=(
        "True if dataset may be overwritten (defaults to False). Production"
        "datasets cannot be overwritten."
    ),
    action="store_true",
)
arg_register_dataset.add_argument(
    "--old_location",
    help=(
        "Absolute location of dataset to copy.\nIf None dataset should already"
        "be at correct relative_path."
    ),
    type=str,
)
arg_register_dataset.add_argument(
    "--make_symlink",
    help="Flag to make symlink to data rather than copy any files.",
    action="store_true",
)
arg_register_dataset.add_argument(
    "--is_dummy",
    help="True for “dummy” datasets (no data is copied, for testing purposes only)",
    action="store_true",
)
arg_register_dataset.add_argument(
    "--schema-version", default=f"{SCHEMA_VERSION}", help="Which schema to connect to",
)
arg_register_dataset.add_argument(
    "--locale", help="Location where dataset was produced", type=str, default="NERSC",
)
arg_register_dataset.add_argument("--owner", help="Owner of dataset. Defaults to $USER.")
arg_register_dataset.add_argument(
    "--owner-type", choices=["production", "group", "user"], default="user"
)
arg_register_dataset.add_argument(
    "--config_file", help="Location of DREGS config file", type=str
)


def main():
    args = parser.parse_args()

    # Register a new entry
    if args.subcommand == "register":
        if args.register_type == "dataset":
            register_dataset(args)