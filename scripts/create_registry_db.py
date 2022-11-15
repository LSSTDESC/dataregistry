import os
import enum
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum
from dataregistry.db_basic import create_db_engine, TableCreator

engine = create_db_engine(config_file=os.path.join(os.getenv('HOME'),
                                                   '.registry_config_dev'))

tab_creator = TableCreator(engine, 'registry_0_1')
class OwnershipEnum(enum.Enum):
    production = 1
    group = 2
    user = 3

# Main table, a row per dataset
cols = []
cols.append(Column("dataset_id", Integer, primary_key=True))
cols.append(Column("name", String, nullable=False))
cols.append(Column("relative_path", String, nullable=False))
cols.append(Column("version_major", Integer, nullable=False))
cols.append(Column("version_minor", Integer, nullable=False))
cols.append(Column("version_patch", Integer, nullable=False))
cols.append(Column("version_suffix", String))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("dataset_creation_date", DateTime))
cols.append(Column("creator_uid", String(20), nullable=False))

# Make access_API a string for now, but it could be an enumeration or
# a foreign key into another table.   Possible values for the column
# might include "gcr-catalogs", "skyCatalogs"
cols.append(Column("access_API", String(20)))

# A way to associate a dataset with a program execution or "run"
cols.append(Column("execution_id", Integer, ForeignKey("execution.execution_id")))
cols.append(Column("description", String))
cols.append(Column("ownership_type", Enum(OwnershipEnum), nullable=False))
# If ownership_type is 'production', then owner is always 'production'
# If ownership_type is 'group', owner will be a group name
# If ownership_type is 'user', owner will be a user name
cols.append(Column("owner", String, nullable=False))

tab_creator.define_table("dataset", cols)

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

tab_creator.define_table("execution", cols)

# Internal dependencies - which datasets are inputs to creation of others
cols = []
cols.append(Column("dependency_id", Integer, primary_key=True))
cols.append(Column("register_date", DateTime, nullable=False))
cols.append(Column("input_id", Integer, ForeignKey("dataset.dataset_id")))
cols.append(Column("output_id", Integer, ForeignKey("dataset.dataset_id")))

tab_creator.define_table("dependency", cols)

tab_creator.create_all()
