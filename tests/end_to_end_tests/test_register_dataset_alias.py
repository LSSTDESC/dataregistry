import os
import sys

import pytest
import yaml
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION

from database_test_utils import *


def test_register_dataset_alias(dummy_file):
    """Register a dataset and make a dataset alias entry for it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Add dataset
    d_id = _insert_dataset_entry(
        datareg,
        "alias_test_entry",
        "0.0.1",
    )

    # Add alias
    _insert_alias_entry(datareg, "nice_dataset_name", d_id)

    # Query
    f = datareg.Query.gen_filter("dataset_alias.alias", "==", "nice_dataset_name")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset_alias.dataset_id",
        ],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert i < 1
        assert getattr(r, "dataset.dataset_id") == d_id
        assert getattr(r, "dataset_alias.dataset_id") == d_id
