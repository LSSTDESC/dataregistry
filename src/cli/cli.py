import os
import sys
import argparse
from dataregistry.db_basic import SCHEMA_VERSION
from .register import register_dataset
from .query import dregs_ls
from dataregistry.schema import load_schema

# ---------------------
# The data registry CLI
# ---------------------
parser = argparse.ArgumentParser(
    description="The data registry CLI interface",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
subparsers = parser.add_subparsers(title="subcommand", dest="subcommand")

# ----------
# Query (ls)
# ----------

# List your entries in the database
arg_ls = subparsers.add_parser("ls", help="List your entries in the data registry")

arg_ls.add_argument("--owner", help="List datasets for a given owner")
arg_ls.add_argument(
    "--owner_type",
    help="List datasets for a given owner type",
    choices=["user", "group", "production"],
)
arg_ls.add_argument(
    "--config_file", help="Location of data registry config file", type=str
)
arg_ls.add_argument("--all", help="List all datasets", action="store_true")
arg_ls.add_argument(
    "--schema",
    default=f"{SCHEMA_VERSION}",
    help="Which schema to connect to",
)
arg_ls.add_argument("--root_dir", help="Location of the root_dir", type=str)
arg_ls.add_argument(
    "--site", help="Get the root_dir through a pre-defined 'site'", type=str
)

# ------------------
# Register a dataset
# ------------------

# Load the schema information
schema_data = load_schema()

# Conversion from string types in `schema.yaml` to SQLAlchemy
_TYPE_TRANSLATE = {
    "String": str,
    "Integer": int,
    "DateTime": str,
    "StringShort": str,
    "StringLong": str,
    "Boolean": bool,
    "Float": float,
}

# Register a new database entry.
arg_register = subparsers.add_parser(
    "register", help="Register a new entry to the database"
)

arg_register_sub = arg_register.add_subparsers(
    title="register what?", dest="register_type"
)

# Register a new dataset.
arg_register_dataset = arg_register_sub.add_parser("dataset", help="Register a dataset")

# Get some information from the `schema.yaml` file
for row in schema_data["dataset"]:
    # Any default?
    if schema_data["dataset"][row]["cli_default"] is not None:
        default = schema_data["dataset"][row]["cli_default"]
        default_str = f" (default={default})"
    else:
        default = None
        default_str = ""

    # Restricted to choices?
    if schema_data["dataset"][row]["choices"] is not None:
        choices = schema_data["dataset"][row]["choices"]
    else:
        choices = None

    # Add flag
    if schema_data["dataset"][row]["cli_optional"]:
        arg_register_dataset.add_argument(
            "--" + row,
            help=schema_data["dataset"][row]["description"] + default_str,
            default=default,
            choices=choices,
            type=_TYPE_TRANSLATE[schema_data["dataset"][row]["type"]],
        )

# Entries unique to registering the dataset using the CLI
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
    "--schema",
    default=f"{SCHEMA_VERSION}",
    help="Which schema to connect to",
)
arg_register_dataset.add_argument(
    "--config_file", help="Location of data registry config file", type=str
)
arg_register_dataset.add_argument(
    "--execution_name", help="Typically pipeline name or program name", type=str
)
arg_register_dataset.add_argument(
    "--execution_description", help="Human readible description of execution", type=str
)
arg_register_dataset.add_argument(
    "--execution_start", help="Date the execution started"
)
arg_register_dataset.add_argument(
    "--execution_locale", help="Where was the execution performed?", type=str
)
arg_register_dataset.add_argument(
    "--execution_configuration",
    help="Path to text file used to configure the execution",
    type=str,
)
arg_register_dataset.add_argument(
    "--input_datasets",
    help="List of dataset ids that were the input to this execution",
    type=int,
    default=[],
    nargs="+",
)
arg_register_dataset.add_argument(
    "--root_dir", help="Location of the root_dir", type=str
)
arg_register_dataset.add_argument(
    "--site", help="Get the root_dir through a pre-defined 'site'", type=str
)


def main():
    args = parser.parse_args()

    # Register a new entry
    if args.subcommand == "register":
        if args.register_type == "dataset":
            register_dataset(args)

    # Query database entries
    elif args.subcommand == "ls":
        dregs_ls(args)
