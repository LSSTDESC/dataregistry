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
parser.add_argument("schema",
                    help="name of schema whose tables are to be modified.")

home = os.getenv('HOME')
alt_config = os.path.join(home, '.config_prod_alt_admin')
parser.add_argument("--config", help="Path to the data registry config file. Determines database (regular or alt) to be modified", default=alt_config)
parser.add_argument("--steps", choices=['mod_schema', 'mod_data', 'both'],
                    default='mod_schema')
args = parser.parse_args()


if  args.schema.endswith('production'):
    assoc_production = args.schema
    entry_mode = 'production'
elif args.schema.endswith('working'):
    assoc_production = args.schema.replace('working', 'production')
    entry_mode = 'working'
else:
    raise ValueError('Schema name must end with "production" or "working"')

query_mode = entry_mode

db_connection = DbConnection(schema=args.schema, config_file=args.config,
                             entry_mode=entry_mode, query_mode=query_mode)

if args.steps in ['mod_schema', 'both']:
    # Update the schema:
    # alter_col = f"alter table {args.schema}.dataset alter column relative_path drop not null"
    alter_table = f"alter table {args.schema}.dataset add column original_path type varchar, add column original_endpoint varchar, add column fetch_date timestamp, add column restore_date timestamp"

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
    upd_col = f" update {args.schema}.dataset set fetch_date = {args.schema}.dataset.register_date"

    print("to be executed: ", upd_col)
    with db_connection.engine.connect() as conn:
        conn.execute(text(upd_col))
        conn.commit()
