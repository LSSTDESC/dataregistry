from dataregistry.query import Query, Filter
from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION
import os

NUM_DATASET_COLUMNS = 23
NUM_EXECUTION_COLUMNS = 7

if os.getenv("DREGS_CONFIG") is None:
    raise Exception("Need to set DREGS_CONFIG env variable")
else:
    DREGS_CONFIG = os.getenv("DREGS_CONFIG")

# Establish connection to database
engine, dialect = create_db_engine(config_file=DREGS_CONFIG)

# Create query object
q = Query(engine, dialect, schema_version=SCHEMA_VERSION)

# Make sure we find the correct number of columns in the dataset and execution tables
props = q.list_dataset_properties()
dataset_columns = [x for x in props if "dataset." in x]
execution_columns = [x for x in props if "execution." in x]
assert len(dataset_columns) == NUM_DATASET_COLUMNS, "Bad dataset columns length"
assert len(execution_columns) == NUM_EXECUTION_COLUMNS, "Bad execution columns length"

# Do some queries on the test data.

# Query on dataset name
f = Filter('dataset.name', '==', 'DESC dataset 1')
results = q.find_datasets(['dataset.dataset_id', 'dataset.name'], [f])
assert results.rowcount == 2, "Bad result from query 1"

# Query on version
f = Filter('dataset.version_major', '<', 100)
results = q.find_datasets(['dataset.dataset_id', 'dataset.name'], [f])
assert results.rowcount == 4, "Bad result from query 2"

# Query to check dataset return type
f = Filter('dataset.version_major', '<', 100)
results = q.find_datasets(['dataset.dataset_id', 'dataset.name'], [f], return_type="dict")
assert type(results) == dict, "Bad return type from query 3"
assert len(results.keys()) == 2, "Bad dict length in query 3"
assert len(results["dataset_id"]) == 4, "Bad array length in query 3"
