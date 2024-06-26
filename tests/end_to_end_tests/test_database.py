import os
import pandas as pd
import sqlalchemy
from sqlalchemy import inspect

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
