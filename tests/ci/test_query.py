import os

from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine, ownertypeenum
from dataregistry.query import Filter, Query

if os.getenv("DREGS_CONFIG") is None:
    raise Exception("Need to set DREGS_CONFIG env variable")
else:
    DREGS_CONFIG = os.getenv("DREGS_CONFIG")

# Establish connection to database
engine, dialect = create_db_engine(config_file=DREGS_CONFIG)

# Create query object
q = Query(engine, dialect, schema_version=SCHEMA_VERSION)

# Do some queries on the test data.

# Query 1: Query on dataset name
f = Filter("dataset.name", "==", "bumped_dataset")
results = q.find_datasets(
    ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
)
assert results.rowcount == 4, "Bad result from query 1"

# Make sure versions (from bump) are correct
for r in results:
    if r.relative_path == "DESC/datasets/bumped_dataset":
        assert r.version_string == "0.0.1"
    elif r.relative_path == "DESC/datasets/bumped_dataset_2":
        assert r.version_string == "0.0.2"
    elif r.relative_path == "DESC/datasets/bumped_dataset_3":
        assert r.version_string == "0.1.0"
    elif r.relative_path == "DESC/datasets/bumped_dataset_4":
        assert r.version_string == "1.0.0"

# Query 2: Query on owner type
f = Filter("dataset.owner_type", "!=", "user")
results = q.find_datasets(["dataset.name"], [f])
assert results.rowcount == 3, "Bad result from query 2"

# Query 3: Query on dataset alias
f = Filter("dataset_alias.alias", "==", "nice_dataset_name")
results = q.find_datasets(["dataset.dataset_id", "dataset_alias.dataset_id"], [f])
assert results.rowcount == 1, "Bad result from query 3"

# Make sure IDs match up
for r in results:
    assert r[0] == r[1]

# Query 4: Make sure auto generated name is correct
f = Filter("dataset.relative_path", "==", "DESC/datasets/my_first_dataset")
results = q.find_datasets(["dataset.name"], [f])
assert results.rowcount == 1, "Bad result from query 4"
for r in results:
    assert r.name == "my_first_dataset", "Bad result from query 4"

# Query 5: Make sure manual name is correct
f = Filter("dataset.relative_path", "==", "DESC/datasets/my_first_named_dataset")
results = q.find_datasets(["dataset.name"], [f])
assert results.rowcount == 1, "Bad result from query 5"
for r in results:
    assert r.name == "named_dataset", "Bad result from query 5"

# Query 6: Find the dependencies of an execution
f = Filter("execution.name", "==", "pipeline_stage_3")
results = q.find_datasets(["execution.execution_id"], [f])
assert results.rowcount == 1, "Bad result from query 6"

f = Filter("dependency.execution_id", "==", next(results)[0])
results = q.find_datasets(["dependency.input_id"], [f])
assert results.rowcount == 2, "Bad result from query 6"

# Query 7: Query on version suffix
f = Filter("dataset.version_suffix", "==", "test-suffix")
results = q.find_datasets(
    ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
)
assert results.rowcount == 2, "Bad result from query 7"

# Make sure versions (from bump) are correct
for r in results:
    if r.relative_path == "DESC/datasets/my_first_suffix_dataset":
        assert r.version_string == "0.0.1"
    elif r.relative_path == "DESC/datasets/my_first_suffix_dataset_bumped":
        assert r.version_string == "0.1.0"
