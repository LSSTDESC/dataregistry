import os
import sys
import argparse
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, Float
from sqlalchemy import ForeignKey, UniqueConstraint
from dataregistry.db_basic import DbConnection, TableCreator, SCHEMA_VERSION
from dataregistry.db_basic import add_table_row, _insert_provenance

# The following should be adjusted whenever there is a change to the structure
# of the database tables.
_DB_VERSION_MAJOR = 1
_DB_VERSION_MINOR = 2
_DB_VERSION_PATCH = 0
_DB_VERSION_COMMENT = "Added comment column to Provenance table"

parser = argparse.ArgumentParser(description='''
Creates dataregistry tables in specified schema and connection information (config)''', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--schema', help="name of schema to contain tables. Will be created if it doesn't already exist", default=f"{SCHEMA_VERSION}")
parser.add_argument('--config', help="Path to the data registry config file")

args = parser.parse_args()

##engine, dialect = create_db_engine(config_file=args.config)
db_connection = DbConnection(args.config, args.schema)
##tab_creator = TableCreator(engine, dialect, schema=schema)
tab_creator = TableCreator(db_connection)

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
cols.append(Column("owner_type", String, nullable=False))
# If ownership_type is 'production', then owner is always 'production'
# If ownership_type is 'group', owner will be a group name
# If ownership_type is 'user', owner will be a user name
cols.append(Column("owner", String, nullable=False))

# To store metadata about the dataset.
cols.append(Column("data_org", String, nullable=False))
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
cols.append(Column("git_hash", String, nullable=True))
cols.append(Column("repo_is_clean", Boolean, nullable=True))
# update method is always "CREATE" for this script.
# Alternative could be "MODIFY" or "MIGRATE"
cols.append(Column("update_method", String(10), nullable=False))
cols.append(Column("schema_enabled_date", DateTime, nullable=False))
cols.append(Column("creator_uid", String(20), nullable=False))
cols.append(Column("comment", String(250)))
tab_creator.define_table("provenance", cols)

tab_creator.create_all()

prov_id = _insert_provenance(db_connection, _DB_VERSION_MAJOR,
                             _DB_VERSION_MINOR, _DB_VERSION_PATCH,
                             "CREATE", comment=_DB_VERSION_COMMENT)
