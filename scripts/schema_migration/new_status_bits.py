import os
import sys
import argparse
import yaml
import logging
from sqlalchemy import text
from dataregistry.db_basic import DbConnection
from dataregistry.db_basic import _insert_provenance
from dataregistry.schema.schema_version import (
    _DB_VERSION_MAJOR,
    _DB_VERSION_MINOR,
    _DB_VERSION_PATCH,
    _DB_VERSION_COMMENT
)

PRODUCTION_HOST = "dataregistry-prod-loadbalancer.desc-dataregistry.production.svc.spin.nersc.org"
PRODUCTION_DB = "desc_data_registry"
PRODUCTION_NAMESPACE = "lsst_desc"

# status bit definitions for code version to follow 1.11.1 (probably 1.12.0)
REGISTERED = 1
DELETED = 2
ARCHIVED = 4
REPLACED = 8
DATA_MANAGED = 16
DATA_DELETED = 32

class ParsedConfig():
    def __init__(self, fpath):
        with open(fpath) as f:
            conn_parms = yaml.safe_load(f)["sqlalchemy.url"]
            self.db = conn_parms.split("/")[-1]
            at_ix = conn_parms.find("@")
            strip1 = conn_parms[at_ix + 1:]
            colon_ix = strip1.find(":")
            self.host = strip1[:colon_ix]


parser = argparse.ArgumentParser(
    description="Update specified schema, using specified config, adding columns to support fetch, archive and restore operations",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--namespace", default="alt",
                    help="namespace schema belongs to")
parser.add_argument("--schema-type", choices=["production", "working"],
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

parsed_config = ParsedConfig(args.config)
prod_host = " (production)" if parsed_config.host == PRODUCTION_HOST else ""
prod_db = " (production)" if parsed_config.db == PRODUCTION_DB else ""
prod_namespace = " (production)" if args.namespace == PRODUCTION_NAMESPACE else ""

print("Db server host is ", parsed_config.host, prod_host)
print("Database is ", parsed_config.db, prod_db)
print("Namespace is ", args.namespace, prod_namespace)
print("Schema type is ", args.schema_type)
print("Steps to execute:  ", args.steps)

response = input("Do you wish to continue? (y or n)")
response = response.lower()[0]
if response != "y":
    print("Bye")
    sys.exit()
else:
    print("Continuing...")

db_connection = DbConnection(schema=schema, config_file=args.config,
                             entry_mode=entry_mode, query_mode=query_mode,
                             logging_level=logging.DEBUG)
logger = db_connection.logger
engine = db_connection.engine

if args.steps in ['mod_schema', 'both']:
    # Update the schema, sort of.  We're not really changing the schema;
    # just the meaning of some bits in dataset.status.  But we still need
    # to add a row to the provenance table
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
    # Fix up status in existing dataset entries.
    schema = args.namespace + '_' + entry_mode

    # OR in manage bit for successfully reg. datasets with managed data
    # (that is, with location_type == "dataregistry")
    manage_stmt = f"update {schema}.dataset set status = status | {DATA_MANAGED} "
    manage_where = " where (location_type = 'dataregistry') and "
    manage_where = manage_where + f" (status & {REGISTERED} = {REGISTERED}) "
    manage_stmt = manage_stmt + manage_where

    # OR in DELETED bit for datasets which have been replaced (because
    # of bug in old `replace` code)
    replace_stmt = f"update {schema}.dataset set status = status | {DELETED} "
    replace_where = f" where (status & {REPLACED} = {REPLACED})"
    replace_stmt += replace_where

    # OR in DATA_DELETED bit for deleted datasets with managed data
    del_stmt = f"update {schema}.dataset set status = status | {DATA_DELETED} "
    del_where =  " where (location_type = 'dataregistry') and "
    del_where = del_where + f" (status & {DELETED} = {DELETED})"
    del_stmt = del_stmt + del_where

    for stmt in [manage_stmt, replace_stmt, del_stmt]:
        logger.debug(f"Executing {stmt}")
        with engine.connect() as conn:
            results = conn.execute(text(stmt))
            conn.commit()
