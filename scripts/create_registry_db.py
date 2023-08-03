import os
import sys
import enum
import argparse
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, Float
from sqlalchemy import ForeignKey, UniqueConstraint, Enum
from dataregistry.db_basic import create_db_engine, TableCreator, ownertypeenum, dataorgenum, add_table_row, SCHEMA_VERSION
from dataregistry.git_util import get_git_info
from dataregistry import __version__

# The following should be adjusted whenever there is a change to the structure
# of the database tables.
_DB_VERSION_MAJOR = 1
_DB_VERSION_MINOR = 0
_DB_VERSION_PATCH = 0

parser = argparse.ArgumentParser(description='''
Creates dataregistry tables in specified schema and connection information (config)''', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--schema', help="name of schema to contain tables. Will be created if it doesn't already exist", default=f"{SCHEMA_VERSION}")
parser.add_argument('--config', help="Path to the DREGS config file")

args = parser.parse_args()
schema = args.schema

engine, dialect = create_db_engine(config_file=args.config)
if dialect == 'sqlite':
    schema = None

tab_creator = TableCreator(engine, dialect, schema=schema)

# Main table, a row per dataset
cols = []
cols.append(Column("dataset_id", Integer, primary_key=True))
cols.append(Column("name", String, nullable=False))
cols.append(Column("relative_path", String, nullable=False))
cols.append(Column("version_major", Integer, nullable=False))
cols.append(Column("version_minor", Integer, nullable=False))
cols.append(Column("version_patch", Integer, nullable=False))
cols.append(Column("version_string", String, nullable=False))
cols.append(Column("version_suffix", String))
cols.append(Column("dataset_creation_date", DateTime))
cols.append(Column("is_archived", Boolean, default=False))
cols.append(Column("is_external_link", Boolean, default=False))
cols.append(Column("is_overwritable", Boolean, default=False))
cols.append(Column("is_overwritten", Boolean, default=False))
cols.append(Column("is_valid", Boolean, default=True)) # False if, e.g., copy failed

# The following are boilerplate, included in all or most tables
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("creator_uid", String(20), nullable=False))

# Make access_API a string for now, but it could be an enumeration or
# a foreign key into another table.   Possible values for the column
# might include "gcr-catalogs", "skyCatalogs"
cols.append(Column("access_API", String(20)))

# A way to associate a dataset with a program execution or "run"
cols.append(Column("execution_id", Integer, ForeignKey("execution.execution_id")))
cols.append(Column("description", String))
cols.append(Column("owner_type", Enum(ownertypeenum), nullable=False))
# If ownership_type is 'production', then owner is always 'production'
# If ownership_type is 'group', owner will be a group name
# If ownership_type is 'user', owner will be a user name
cols.append(Column("owner", String, nullable=False))

# To store metadata about the dataset.
cols.append(Column("data_org", Enum(dataorgenum), nullable=False))
cols.append(Column("nfiles", Integer, nullable=False))
cols.append(Column("total_disk_space", Float, nullable=False))
tab_creator.define_table("dataset", cols,
                         [Index("relative_path", "owner", "owner_type"),
                          UniqueConstraint("name", "version_string",
                                           "version_suffix",
                                           name="dataset_u_version")])

# Dataset alias name table
cols = []
cols.append(Column("dataset_alias_id", Integer, primary_key=True))
cols.append(Column("alias", String, nullable=False))
cols.append(Column("dataset_id", Integer, ForeignKey("dataset.dataset_id")))
cols.append(Column("supersede_date", DateTime,  default=None))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("creator_uid", String(20), nullable=False))
tab_creator.define_table("dataset_alias", cols,
                         [UniqueConstraint("alias", "register_date",
                                           name="dataset_u_register")])

# Execution table
cols = []
cols.append(Column("execution_id", Integer, primary_key=True))
cols.append(Column("description", String))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("execution_start", DateTime))
# name is meant to identify the code executed.  E.g., could be pipeline name
cols.append(Column("name", String))
# locale is, e.g. site where code was run
cols.append(Column("locale", String))
cols.append(Column("configuration", String))
cols.append(Column("creator_uid", String(20), nullable=False))
tab_creator.define_table("execution", cols)

# Execution alias name table
cols = []
cols.append(Column("execution_alias_id", Integer, primary_key=True))
cols.append(Column("alias", String, nullable=False))
cols.append(Column("execution_id", Integer,
                   ForeignKey("execution.execution_id")))
cols.append(Column("supersede_date", DateTime,  default=None))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("creator_uid", String(20), nullable=False))
tab_creator.define_table("execution_alias", cols,
                         [UniqueConstraint("alias", "register_date",
                                           name="execution_u_register")])

# Internal dependencies - which datasets are inputs to creation of others
# This table associates an execution with its inputs
cols = []
cols.append(Column("dependency_id", Integer, primary_key=True))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("input_id", Integer, ForeignKey("dataset.dataset_id")))
cols.append(Column("execution_id", Integer, ForeignKey("execution.execution_id")))
tab_creator.define_table("dependency", cols)


# Keep track of code version creating the db
# Create this table separately so that we have handle needed to
# make an entry
cols = []
cols.append(Column("provenance_id", Integer, primary_key=True))
cols.append(Column("code_version_major", Integer, nullable=False))
cols.append(Column("code_version_minor", Integer, nullable=False))
cols.append(Column("code_version_patch", Integer, nullable=False))
cols.append(Column("code_version_suffix", String))
cols.append(Column("db_version_major", Integer, nullable=False))
cols.append(Column("db_version_minor", Integer, nullable=False))
cols.append(Column("db_version_patch", Integer, nullable=False))
cols.append(Column("git_hash", String, nullable=False))
cols.append(Column("repo_is_clean", Boolean, nullable=False))
# update method is always "CREATE" for this script.
# Alternative could be "MODIFY" or "MIGRATE"
cols.append(Column("update_method", String(10), nullable=False))
cols.append(Column("schema_enabled_date", DateTime, nullable=False))
cols.append(Column("creator_uid", String(20), nullable=False))
tab_creator.define_table("provenance", cols)

tab_creator.create_all()

# Now insert a row into the provenance table
# First have to get metadata for the table we just created
provenance_table = tab_creator.get_table_metadata("provenance")
version_fields = __version__.split(".")
patch = version_fields[2]
suffix = None
if "-" in patch:
    subfields = patch.split("-")
    patch = subfields[0]
    suffix = "-".join(subfields[1:])

values = dict()
values["code_version_major"] = version_fields[0]
values["code_version_minor"] = version_fields[1]
values["code_version_patch"] = patch
if suffix:
    values["code_version_suffix"] = suffix
values["db_version_major"] = _DB_VERSION_MAJOR
values["db_version_minor"] = _DB_VERSION_MINOR
values["db_version_patch"] = _DB_VERSION_PATCH
values["schema_enabled_date"] = datetime.now()
values["creator_uid"] = os.getenv("USER")
pkg_root =  os.path.join(os.path.dirname(__file__), '..')
git_hash, is_clean = get_git_info(pkg_root)
values["git_hash"] = git_hash
values["repo_is_clean"] = is_clean
values["update_method"] = "CREATE"

with engine.connect() as conn:
    id = add_table_row(conn, provenance_table, values)
