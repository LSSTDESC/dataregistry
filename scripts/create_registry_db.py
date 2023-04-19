import os
import sys
import enum
import argparse
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index
from sqlalchemy import ForeignKey, UniqueConstraint, Enum
from dataregistry.db_basic import create_db_engine, TableCreator, ownertypeenum

parser = argparse.ArgumentParser(description='''
Creates dataregistry tables in specified schema and connection information (config)''', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--schema', help="name of schema to contain tables. Will be created if it doesn't already exist", default="registry_dev")
parser.add_argument('--config', default="", help="path to config file used to establish connection")

args = parser.parse_args()
schema = args.schema
config = args.config
if config == "":
    config = os.path.join(os.getenv('HOME'), '.config_reg_writer')

engine, dialect = create_db_engine(config_file=config)
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
cols.append(Column("owner_type", Enum(ownertypeenum, schema=schema), nullable=False))
# If ownership_type is 'production', then owner is always 'production'
# If ownership_type is 'group', owner will be a group name
# If ownership_type is 'user', owner will be a user name
cols.append(Column("owner", String, nullable=False))


tab_creator.define_table("dataset", cols,
                         [Index("relative_path", "owner", "owner_type")])

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
cols = []
cols.append(Column("dependency_id", Integer, primary_key=True))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("input_id", Integer, ForeignKey("dataset.dataset_id")))
cols.append(Column("output_id", Integer, ForeignKey("dataset.dataset_id")))

tab_creator.define_table("dependency", cols)

### Still to be defined:
#    a table to keep track of external dependencies
#    moved.  A table to track history of datasets which have been moved.

tab_creator.create_all()
