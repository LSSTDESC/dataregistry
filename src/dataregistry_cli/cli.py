import os
import sys
import argparse
from dataregistry.db_basic import SCHEMA_VERSION
from .register import register_dataset
from .delete import delete_dataset
from .query import dregs_ls
from dataregistry.schema import load_schema

def _add_generic_arguments(parser_obj):
    """
    Most commands have the schema, root_dir, etc. as options. This function
    does that for us.

    Parameters
    ----------
    parser_obj : argparse.ArgumentParser
        Argument parser we are adding the options to
    """

    parser_obj.add_argument(
        "--config_file", help="Location of data registry config file", type=str
    )
    parser_obj.add_argument("--root_dir", help="Location of the root_dir", type=str)
    parser_obj.add_argument(
        "--site", help="Get the root_dir through a pre-defined 'site'", type=str
    )
    parser_obj.add_argument(
        "--schema",
        default=f"{SCHEMA_VERSION}",
        help="Which schema to connect to",
    )

def get_parser():

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
    arg_ls.add_argument("--all", help="List all datasets", action="store_true")
    _add_generic_arguments(arg_ls)

    # --------
    # Register
    # --------
    
    # Load the schema information (help strings are loaded from here)
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
   
    # ------------------
    # Register a dataset
    # ------------------

    # Register a new dataset.
    arg_register_dataset = arg_register_sub.add_parser("dataset", help="Register a dataset")
    
    # Get some information from the `schema.yaml` file
    for column in schema_data["dataset"]:
        extra_args = {}
    
        # Any default?
        if schema_data["dataset"][column]["cli_default"] is not None:
            extra_args["default"] = schema_data["dataset"][column]["cli_default"]
            default_str = f" (default={extra_args['default']})"
        else:
            default_str = ""
    
        # Restricted to choices?
        if schema_data["dataset"][column]["choices"] is not None:
            extra_args["choices"] = schema_data["dataset"][column]["choices"]
    
        # Is this a boolean flag?
        if schema_data["dataset"][column]["type"] == "Boolean":
            extra_args["action"] = "store_true"
        else:
            extra_args["type"] = _TYPE_TRANSLATE[schema_data["dataset"][column]["type"]]
    
        # Add flag
        if schema_data["dataset"][column]["cli_optional"]:
            arg_register_dataset.add_argument(
                "--" + column,
                help=schema_data["dataset"][column]["description"] + default_str,
                **extra_args,
            )
    
    # Entries unique to registering the dataset when using the CLI
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
        "--schema",
        default=f"{SCHEMA_VERSION}",
        help="Which schema to connect to",
    )
    arg_register_dataset.add_argument(
        "--config_file", help="Location of data registry config file", type=str
    )
    arg_register_dataset.add_argument(
        "--location_type", type=str, default="onsite"
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
        "--execution_site", help="Where was the execution performed?", type=str
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
    _add_generic_arguments(arg_register_dataset)

    # ------
    # Delete
    # ------
    
    # Register a new database entry.
    arg_delete = subparsers.add_parser(
        "delete", help="Delete an entry in the database"
    )
    
    arg_delete_sub = arg_delete.add_subparsers(
        title="delete what?", dest="delete_type"
    )

    # ----------------
    # Delete a dataset
    # ----------------

    # Delete a dataset.
    arg_delete_dataset = arg_delete_sub.add_parser("dataset", help="Delete a dataset")
    arg_delete_dataset.add_argument("dataset_id", help="The dataset_id you wish to delete",
            type=int)
    _add_generic_arguments(arg_delete_dataset)

    return parser

def parse_args(args):
    """
    Parse a list of arguments.

    Parameters
    ----------
    args : list
        Argument list, when None sys.argv is used
    """

    parser = get_parser()

    return parser.parse_args(args)

def main(cmd=None):

    args = parse_args(cmd)

    # Register a new entry
    if args.subcommand == "register":
        if args.register_type == "dataset":
            register_dataset(args)

    # Delete an entry
    elif args.subcommand == "delete":
        if args.delete_type == "dataset":
            delete_dataset(args)

    # Query database entries
    elif args.subcommand == "ls":
        dregs_ls(args)
