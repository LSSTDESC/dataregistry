import os

from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine, ownertypeenum
from dataregistry.query import Filter, Query

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

# Query 1: Query dataset name
f = Filter("dataset.name", "==", "DESC dataset 1")
results = q.find_datasets(["dataset.dataset_id", "dataset.name"], [f])
assert results.rowcount == 2, "Bad result from query 1"

# Query 2: Query on version
f = Filter("dataset.version_major", "<", 100)
results = q.find_datasets(["dataset.dataset_id", "dataset.name"], [f])
assert results.rowcount == 4, "Bad result from query 2"

# Query 3: Query on execution ID
f = Filter("dataset.execution_id", "==", 1)
results = q.find_datasets(["execution.execution_id", "dataset.execution_id"], [f])
assert results.rowcount == 1, "Bad result from query 3.1"
for r in results:
    assert r[0] == r[1], "Bad result from query 3.2"
f = Filter("execution.execution_id", "==", 1)
results = q.find_datasets(["execution.execution_id", "dataset.execution_id"], [f])
assert results.rowcount == 1, "Bad result from query 3.3"
for r in results:
    assert r[0] == r[1], "Bad result from query 3.4"

## Query 4: Query on alias
#f = Filter("dataset_alias.alias", "==", "My first alias")
#results = q.find_datasets(["dataset.dataset_id", "dataset_alias.alias"], [f])
#assert results.rowcount == 1, "Bad result from query 4"
