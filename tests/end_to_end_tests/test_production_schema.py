import os
import sys

import pytest
import yaml
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION, DbConnection

from database_test_utils import *

# This is just to see what backend we are using
# Remember no production schema when using sqlite backend
db_connection = DbConnection(None, schema=SCHEMA_VERSION)


@pytest.mark.skipif(
    db_connection._dialect == "sqlite", reason="no production with sqlite"
)
def test_register_with_production_dependencies(dummy_file):
    """Test registering an entry in the production schema"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)
    datareg_prod = DataRegistry(root_dir=str(tmp_root_dir), schema="production")

    # Make a dataset in each schema
    d_id_prod = _insert_dataset_entry(
        datareg_prod,
        "DESC:datasets:production_dataset_for_deps",
        "0.0.1",
        owner="production",
        owner_type="production",
    )

    d_id = _insert_dataset_entry(
        datareg,
        "DESC:datasets:nonproduction_dataset_for_deps",
        "0.0.1",
    )

    # Make a execution with input datasets from each schema
    ex_id = _insert_execution_entry(
        datareg,
        "execution_with_production_dependencies",
        "an execution with production dependencies",
        input_datasets=[d_id],
        input_production_datasets=[d_id_prod],
    )

    # Check dependencies were made
    f = datareg.Query.gen_filter("dependency.execution_id", "==", ex_id)
    results = datareg.Query.find_datasets(
        [
            "dependency.input_id",
            "dependency.input_production_id",
        ],
        [f],
        return_format="cursorresult",
    )

    assert len(list(results)) == 2
    for i, r in enumerate(results):
        if i == 0:
            assert getattr(r, "dependency.input_id") == d_id
            assert getattr(r, "dependency.input_production_id") is None
        else:
            assert getattr(r, "dependency.input_id") == None
            assert getattr(r, "dependency.input_production_id") is d_id_prod


@pytest.mark.skipif(
    db_connection._dialect == "sqlite", reason="no production with sqlite"
)
def test_production_schema_register(dummy_file):
    """Test registering an entry in the production schema"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema="production")

    d_id = _insert_dataset_entry(
        datareg,
        "DESC:datasets:production_dataset_1",
        "0.0.1",
        owner="production",
        owner_type="production",
    )

    # Query
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        [
            "dataset.owner",
            "dataset.owner_type",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert i < 1
        assert getattr(r, "dataset.owner") == "production"
        assert getattr(r, "dataset.owner_type") == "production"


@pytest.mark.skipif(
    db_connection._dialect == "sqlite", reason="no production with sqlite"
)
def test_production_schema_bad_register(dummy_file):
    """Test registering a bad entry in the production schema"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema="production")

    # Try to register dataset without production owner type
    with pytest.raises(ValueError, match="can go in the production schema"):
        d_id = _insert_dataset_entry(
            datareg,
            "DESC:datasets:bad_production_dataset_1",
            "0.0.1",
            owner_type="user",
        )

    # Try to enter overwritable dataset
    with pytest.raises(ValueError, match="Cannot overwrite production entries"):
        d_id = _insert_dataset_entry(
            datareg,
            "DESC:datasets:bad_production_dataset_2",
            "0.0.1",
            owner_type="production",
            is_overwritable=True,
        )

    # Try to have a version suffix
    with pytest.raises(
        ValueError, match="Production entries can't have version suffix"
    ):
        d_id = _insert_dataset_entry(
            datareg,
            "DESC:datasets:bad_production_dataset_3",
            "0.0.1",
            owner="production",
            owner_type="production",
            version_suffix="prod",
        )
