import os
import argparse
from sqlalchemy import text
from dataregistry.db_basic import DbConnection
from dataregistry.db_basic import _insert_provenance
from dataregistry.schema.schema_version import (
    _DB_VERSION_MAJOR,
    _DB_VERSION_MINOR,
    _DB_VERSION_PATCH,
    _DB_VERSION_COMMENT
)

parser = argparse.ArgumentParser(
    description="Update specified schema, using specified config, adding columns to support fetch, archive and restore operations",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--namespace", default="alt",
                    help="namespace schema belongs to")
parser.add_argument("--schema_type", choices=["production", "working"],
                    help="type of schema whose tables are to be modified.")

home = os.getenv('HOME')
admin_config = os.path.join(home, '.dataregistry_admin_config')
alt_admin_config = os.path.join(home, '.alt_admin_config')
parser.add_argument("--config", help="Path to the data registry config file. Determines database (regular or alt) to be modified", default=alt_admin_config)
parser.add_argument("--steps", choices=['mod_schema', 'mod_data', 'both'],
                    default='mod_schema')
args = parser.parse_args()

schema = args.namespace + '_' + args.schema_type

entry_mode = args.schema_type
query_mode = entry_mode
assoc_production = args.namespace + '_production'

db_connection = DbConnection(schema=schema, config_file=args.config,
                             entry_mode=entry_mode, query_mode=query_mode)

if args.steps in ['mod_schema', 'both']:
    # Update the schema:
    # Add in new archive table
    arch_cols = ["archive_id integer primary key",
                 "archive_type character varying",
                 "archive_status integer",
                 "archive_date timestamp",
                 "archive_path character varying",
                 ]
    col_defs = ",".join(arch_cols)
    create_arch_tbl = f"create table {schema}.archive ({col_defs})"

    print("To be executed: ", create_arch_tbl)
    with db_connection.engine.connect() as conn:
        conn.execute(text(create_arch_tbl))
        conn.commit()

    # Remove unused archive-related columns from dataset
    # Add bookkeeping columns to dataset
    # Add foreign key, referencing archive, to dataset
    dataset_actions = ["add column fetch_date timestamp",
                       "add column restore_date timestamp",
                       "drop column archive_path",
                       "drop column archive_date",
                       "add column archive_id integer",]
    constraints = f"add constraint dataset_archive_id_fkey foreign key (archive_id) references {schema}.archive(archive_id)"
    joined_actions = ",".join(dataset_actions) + ","
    alter_table = f"alter table {schema}.dataset {joined_actions} {constraints}"

    print("To be executed: ", alter_table)
    with db_connection.engine.connect() as conn:
        conn.execute(text(alter_table))
        conn.commit()

    # If we got this far add a row to the provenance table
    _insert_provenance(
        db_connection,
        _DB_VERSION_MAJOR,
        _DB_VERSION_MINOR,
        _DB_VERSION_PATCH,
        "MIGRATE",
        comment=_DB_VERSION_COMMENT,
        associated_production=assoc_production
    )
if args.steps in ['mod_data', 'both']:
    # Update fetch_date to match register_date
    upd_col = f" update {schema}.dataset set fetch_date = {schema}.dataset.register_date"

    print("to be executed: ", upd_col)
    with db_connection.engine.connect() as conn:
        conn.execute(text(upd_col))
        conn.commit()
