import os
import sys

import pandas as pd
import pytest
import sqlalchemy
import yaml
from dataregistry import DataRegistry
from dataregistry.db_basic import SCHEMA_VERSION
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


def test_register_dataset_with_bad_keywords(dummy_file):
    """Register a dataset with bad keywords"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Test case where keywords are not strings
    with pytest.raises(ValueError, match="not a valid keyword string"):
        _ = _insert_dataset_entry(
            datareg,
            "DESC/datasets/my_first_dataset_with_bad_keywords",
            "0.0.1",
            keywords=[10,20]
        )

    # Test case where keywords are not previously registered in keyword table
    with pytest.raises(ValueError, match="Not all keywords"):
        _ = _insert_dataset_entry(
            datareg,
            "DESC/datasets/my_second_dataset_with_bad_keywords",
            "0.0.1",
            keywords=["bad_keyword"]
        )

def test_register_dataset_with_keywords(dummy_file):
    """Register a dataset with keywords, then query using keywords"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=SCHEMA_VERSION)

    # Register a dataset with keywords
    d_id = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_first_dataset_with_keywords",
        "0.0.1",
        keywords=["simulation", "observation"]
    )

    d_id2 = _insert_dataset_entry(
        datareg,
        "DESC/datasets/my_second_dataset_with_keywords",
        "0.0.1",
        keywords=["simulation"]
    )

    # Query using keywords
    f = datareg.Query.gen_filter("keyword.keyword", "==", "simulation")
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        assert getattr(r, "dataset.dataset_id") in [d_id, d_id2]
        assert getattr(r, "keyword.keyword") == "simulation"
