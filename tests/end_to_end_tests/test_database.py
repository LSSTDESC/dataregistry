import os
import pandas as pd
import sqlalchemy

from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir="temp")


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
    """Test a query where no properties are chosen, i.e., 'return *'"""

    results = datareg.Query.find_datasets()

    assert results is not None


def test_db_version():
    """
    Test if db version is what we think it should be.
    CI makes a fresh database, hence actual db versions should match
    versions to be used when new db is created
    """
    actual_major, actual_minor, actual_patch = datareg.Query.get_db_versioning()
    assert actual_major == 2, "db major version doesn't match expected"
    assert actual_minor == 3, "db minor version doesn't match expected"
    assert actual_patch == 0, "db patch version doesn't match expected"
