import os
import sys
import argparse
from dataregistry.schema import DEFAULT_NAMESPACE
from .register import register_dataset
from .delete import delete_dataset
from .query import dregs_ls
from .show import dregs_show
from .modify import modify_dataset
from dataregistry.schema import load_schema


def _add_generic_arguments(parser_obj, add_entry_mode=True, add_query_mode=False):
    """
    Most commands have the schema, root_dir, etc. as options. This function
    does that for us.

    Parameters
    ----------
    parser_obj : argparse.ArgumentParser
        Argument parser we are adding the options to
    add_entry_mode : bool
        True to add entry_mode as an option
    add_query_mode : bool
        True to add query_mode as an option
    """

    parser_obj.add_argument(
        "--config_file",
        help="Path to config file, if None, default location is assumed.",
        type=str
    )
    parser_obj.add_argument("--root_dir", type=str,
            help="""Root directory for datasets, if None, default is assumed.""")
    parser_obj.add_argument(
        "--site", type=str,
        help="""Can be used instead of `root_dir`. Some predefined "sites" are
            built in, such as "nersc", which will set the `root_dir` to the
            data registry's default data location at NERSC."""
    )
    parser_obj.add_argument(
        "--schema",
        help="""Schema to connect to, to connect directly to a chosen schema,
            bypassing the namespace.""",
    )
    parser_obj.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help="""Namespace to connect to. If None, the default namespace will be
            used.""",
    )
    if add_entry_mode:
        parser_obj.add_argument(
            "--entry_mode",
            default="working",
            help="""Which schema ('working' or 'production') within the namespace 
            to use when writing/modifying/deleting entries.""",
        )
    if add_query_mode:
        parser_obj.add_argument(
            "--query_mode",
            default="both",
            help="""Which schema(s) ("working" or "production") to probe when querying.
                By default query_mode="both", which searches both schemas together,
                however this can be restricted to either "working" or "production"
                to restrict searches to a single schema."""
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

    # ---------------------
    # Show (show some info)
    # ---------------------

    # List information
    arg_show = subparsers.add_parser("show", help="Show some properties")
    arg_show_sub = arg_show.add_subparsers(title="show what?", dest="show_type")

    # Show list of pre-registered keywords
    arg_show_keywords = arg_show_sub.add_parser(
        "keywords", help="Show list of pre-defined keywords"
    )
    _add_generic_arguments(arg_show_keywords, add_entry_mode=False, add_query_mode=True)

    # ----------
    # Query (ls)
    # ----------

    # List your entries in the database
    arg_ls = subparsers.add_parser("ls", help="List your entries in the data registry")

    arg_ls.add_argument("--owner",
            help="""List datasets for a given owner (default is $USER).
                Selecting '--owner none' will return results from all owners."""
    )
    arg_ls.add_argument(
        "--owner_type",
        help="List datasets for a given owner type",
        choices=["user", "group", "production", "project"],
    )
    arg_ls.add_argument(
        "--name",
        help=""""Only return datasets with a given name. This can be used
        with wildcard support, for example `--name DESC:*`"""
    )
    arg_ls.add_argument(
        "--return_cols",
        help="List of columns from dataset table to return in the query",
        nargs="+",
        type=str,
    )
    arg_ls.add_argument(
        "--max_rows",
        help="Maximum number of rows to print (default 500)",
        type=int,
        default=500,
    )
    arg_ls.add_argument(
        "--max_chars",
        help="Maximum number of characters to print in a column (default 40)",
        type=int,
        default=40,
    )
    arg_ls.add_argument("--keyword", type=str, help="Keyword to filter by")
    _add_generic_arguments(arg_ls, add_entry_mode=False, add_query_mode=True)

    # ------
    # Modify
    # ------

    # Modify a database entry.
    arg_modify = subparsers.add_parser("modify", help="Modify an entry in the database")

    arg_modify_sub = arg_modify.add_subparsers(title="modify what?", dest="modify_type")

    # ------------------
    # Modify a dataset
    # ------------------

    # Modify a dataset entry.
    arg_modify_dataset = arg_modify_sub.add_parser("dataset", help="Modify a dataset")

    arg_modify_dataset.add_argument(
        "dataset_id",
        help="`dataset_id` of dataset to modify",
        type=int,
    )

    arg_modify_dataset.add_argument(
        "column",
        help="Column in the dataset table to modify",
        type=str,
    )

    arg_modify_dataset.add_argument(
        "new_value",
        help="Updated value",
        type=str,
    )
    _add_generic_arguments(arg_modify_dataset)

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
    arg_register_dataset = arg_register_sub.add_parser(
        "dataset", help="Register a dataset"
    )

    # Get some information from the `schema.yaml` file
    for column in schema_data["tables"]["dataset"]["column_definitions"]:
        extra_args = {}

        # Any default?
        if (
            schema_data["tables"]["dataset"]["column_definitions"][column][
                "cli_default"
            ]
            is not None
        ):
            extra_args["default"] = schema_data["tables"]["dataset"][
                "column_definitions"
            ][column]["cli_default"]
            default_str = f" (default={extra_args['default']})"
        else:
            default_str = ""

        # Restricted to choices?
        if (
            schema_data["tables"]["dataset"]["column_definitions"][column]["choices"]
            is not None
        ):
            extra_args["choices"] = schema_data["tables"]["dataset"][
                "column_definitions"
            ][column]["choices"]

        # Is this a boolean flag?
        if (
            schema_data["tables"]["dataset"]["column_definitions"][column]["type"]
            == "Boolean"
        ):
            extra_args["action"] = "store_true"
        else:
            extra_args["type"] = _TYPE_TRANSLATE[
                schema_data["tables"]["dataset"]["column_definitions"][column]["type"]
            ]

        # Add flag
        if schema_data["tables"]["dataset"]["column_definitions"][column][
            "cli_optional"
        ]:
            arg_register_dataset.add_argument(
                "--" + column,
                help=schema_data["tables"]["dataset"]["column_definitions"][column][
                    "description"
                ]
                + default_str,
                **extra_args,
            )

    # Entries unique to registering the dataset when using the CLI
    arg_register_dataset.add_argument(
        "name",
        help="""Any convenient, evocative name for the human. Note the
        combination of name and version must be unique.""",
        type=str,
    )
    arg_register_dataset.add_argument(
        "version",
        help="""Semantic version string of the format MAJOR.MINOR.PATCH or a
        special flag “patch”, “minor” or “major”. When a special flag is used
        it automatically bumps the relative version for you (see examples for
        more details).""",
        type=str,
    )
    arg_register_dataset.add_argument(
        "--old_location",
        help="""Absolute location of dataset to copy. If None dataset should
        already be at correct relative_path.""",
        type=str,
    )
    arg_register_dataset.add_argument(
        "--execution_name", help="Typically pipeline name or program name", type=str
    )
    arg_register_dataset.add_argument(
        "--execution_description",
        help="Human readible description of execution",
        type=str,
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
    arg_register_dataset.add_argument(
        "--keywords",
        help="List of (predefined) keywords to tag dataset with",
        type=str,
        default=[],
        nargs="+",
    )
    _add_generic_arguments(arg_register_dataset)

    # ------
    # Delete
    # ------

    # Register a new database entry.
    arg_delete = subparsers.add_parser("delete", help="Delete an entry in the database")

    arg_delete_sub = arg_delete.add_subparsers(title="delete what?", dest="delete_type")

    # ----------------
    # Delete a dataset
    # ----------------

    # Delete a dataset.
    arg_delete_dataset_id = arg_delete_sub.add_parser(
        "dataset_by_id", help="Delete a dataset using the dataset_id"
    )
    arg_delete_dataset_id.add_argument(
        "dataset_id", help="The dataset_id you wish to delete", type=int
    )
    _add_generic_arguments(arg_delete_dataset_id)

    arg_delete_dataset = arg_delete_sub.add_parser(
        "dataset",
        help="Delete a dataset using the name/version_string/owner/owner_type",
    )
    arg_delete_dataset.add_argument(
        "name", help="The dataset name for the dataset you wish to delete", type=str
    )
    arg_delete_dataset.add_argument(
        "version_string",
        help="The dataset version_string for the dataset you wish to delete",
        type=str,
    )
    arg_delete_dataset.add_argument(
        "owner", help="The dataset owner for the dataset you wish to delete", type=str
    )
    arg_delete_dataset.add_argument(
        "owner_type",
        help="The dataset owner_type for the dataset you wish to delete",
        type=str,
    )
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
        if args.delete_type in ["dataset", "dataset_by_id"]:
            delete_dataset(args)

    # Query database entries
    elif args.subcommand == "ls":
        dregs_ls(args)

    # Show database info
    elif args.subcommand == "show":
        if args.show_type == "keywords":
            dregs_show("keywords", args)

    # Modify an entry
    if args.subcommand == "modify":
        if args.modify_type == "dataset":
            modify_dataset(args)
