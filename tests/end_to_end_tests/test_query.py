import os
import pandas as pd
import sqlalchemy

from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

_TEST_ROOT_DIR = "DataRegistry_data"
_TEST_ROOT_DIR_PRODUCTION = "DataRegistry_data_production"

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir=_TEST_ROOT_DIR)

# Establish connection to database (production schema)
datareg_prod = DataRegistry(root_dir=_TEST_ROOT_DIR_PRODUCTION, schema="production")


def test_query_return_format():
    """Test we get back correct data format from queries"""

    # Default, SQLAlchemy CursorResult
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
        return_format="cursorresult",
    )
    assert type(results) == sqlalchemy.engine.cursor.CursorResult

    # Pandas DataFrame
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
        return_format="dataframe",
    )
    assert type(results) == pd.DataFrame

    # Property dictionary (each key is a property with a list for each row)
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"],
        [],
    )
    assert type(results) == dict


def test_query_all():
    """Test a query where no properties are chosen, i.e., 'return *"""
    results = datareg.Query.find_datasets()

    assert results is not None


#def test_query_dataset_cli():
#    """Test queries for the dataset table entered from the CLI script"""
#
#    # Query 1: Make sure we find all datasets entered using the CLI
#    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset")
#    results = datareg.Query.find_datasets(
#        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
#    )
#    assert len(results["dataset.name"]) == 2, "Bad result from query dcli1"

    # Query 2: Find production dependencies of an execution
    if datareg.Query._dialect != "sqlite":
        f = datareg.Query.gen_filter("execution.name", "==", "production_id_test")
        results = datareg.Query.find_datasets(["execution.execution_id"], [f])
        assert len(results["execution.execution_id"]) == 1, "Bad result from query ex1"

        # Find dependencies for this execution
        f = datareg.Query.gen_filter(
            "dependency.execution_id", "==", results["execution.execution_id"][0]
        )
        results = datareg.Query.find_datasets(["dependency.input_production_id"], [f])
        assert (
            len(results["dependency.input_production_id"]) == 1
        ), "Bad result from query dep2"


def test_db_version():
    """
    Test if db version is what we think it should be.
    CI makes a fresh database, hence actual db versions should match
    versions to be used when new db is created
    """
    actual_major, actual_minor, actual_patch = datareg.Query.get_db_versioning()
    assert actual_major == 1, "db major version doesn't match expected"
    assert actual_minor == 2, "db minor version doesn't match expected"
    assert actual_patch == 0, "db patch version doesn't match expected"


#def test_get_dataset_absolute_path():
#    """Test the generation of the full absolute path of a dataset"""
#
#    dset_relpath = "DESC/datasets/group1_dataset_1"
#    dset_ownertype = "group"
#    dset_owner = "group1"
#    v = datareg.Query.get_dataset_absolute_path(7)
#
#    if datareg.Query._dialect == "sqlite":
#        assert v == os.path.join(
#            _TEST_ROOT_DIR, dset_ownertype, dset_owner, dset_relpath
#        )
#    else:
#        assert v == os.path.join(
#            _TEST_ROOT_DIR, SCHEMA_VERSION, dset_ownertype, dset_owner, dset_relpath
#        )
