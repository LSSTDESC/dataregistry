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

    # Keywords are not strings
    with pytest.raises(ValueError, match="not a valid keyword string"):
        _ = _insert_dataset_entry(
            datareg,
            "DESC/datasets/my_first_dataset_with_bad_keywords",
            "0.0.1",
            keywords=[10,20]
        )

    # Keywords are not registered
    with pytest.raises(ValueError, match="Did not find all keywords"):
        _ = _insert_dataset_entry(
            datareg,
            "DESC/datasets/my_second_dataset_with_bad_keywords",
            "0.0.1",
            keywords=["bad_keyword"]
        )

def test_register_dataset_with_keywords(dummy_file):
    """Register a dataset with keywords"""

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

    # Query for keywords
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        None,
        [f],
        return_format="cursorresult",
    )

    for i, r in enumerate(results):
        print(r)

    ## Query for keywords
    #f = datareg.Query.gen_filter("keyword.keyword", "==", "simulation")
    #results = datareg.Query.find_datasets(
    #    None,
    #    [f],
    #    return_format="cursorresult",
    #)

    #for i, r in enumerate(results):
    #    print(r)
    #    assert getattr(r, "dataset.name") == "my_first_dataset_with_keywords"
    #    assert getattr(r, "dataset.version_string") == "0.0.1"
    #    assert getattr(r, "keyword.keyword") == "simulation"
    #    assert getattr(r, "dataset.dataset_id") == d_id
    #    assert i < 1


