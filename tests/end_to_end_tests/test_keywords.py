import os
import sys

import pandas as pd
import pytest
import sqlalchemy
import yaml
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_SCHEMA_WORKING
from dataregistry.registrar.dataset_util import get_dataset_status, set_dataset_status
from dataregistry.registrar.registrar_util import _form_dataset_path

from database_test_utils import *


def test_register_dataset_with_bad_keywords(dummy_file):
    """
    Make sure we throw exceptions when registering bad keywords

    Case 1) When the keywords aren't strings
    Case 2) When the chosen keyword doesn't exist in the keyword table
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Test case where keywords are not strings
    with pytest.raises(ValueError, match="not a valid keyword string"):
        _ = _insert_dataset_entry(
            datareg,
            "DESC:datasets:my_first_dataset_with_bad_keywords",
            "0.0.1",
            keywords=[10, 20],
        )

    # Test case where keywords are not previously registered in keyword table
    with pytest.raises(ValueError, match="Not all keywords"):
        _ = _insert_dataset_entry(
            datareg,
            "DESC:datasets:my_second_dataset_with_bad_keywords",
            "0.0.1",
            keywords=["bad_keyword"],
        )


def test_register_dataset_with_keywords(dummy_file):
    """
    Register some basic datasets with keywords.

    Then query the registry on a keyword and make sure we get our datasets back.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Register two datasets with keywords
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:datasets:my_first_dataset_with_keywords",
        "0.0.1",
        keywords=["simulation", "observation"],
    )

    d_id2 = _insert_dataset_entry(
        datareg,
        "DESC:datasets:my_second_dataset_with_keywords",
        "0.0.1",
        keywords=["simulation"],
    )

    # Query on the "simulation" keyword
    f = datareg.Query.gen_filter("keyword.keyword", "==", "simulation")
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
        return_format="property_dict",
    )

    # Make sure we get our datasets back
    for tmp_id in [d_id, d_id2]:
        assert tmp_id in results["dataset.dataset_id"]
    for tmp_k in results["keyword.keyword"]:
        assert tmp_k == "simulation"


def test_modify_dataset_with_keywords(dummy_file):
    """
    Register a basic dataset without any keywords.

    Then add keywords with `add_keywords()`.

    Then query to make sure the keyword was tagged.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), schema=DEFAULT_SCHEMA_WORKING)

    # Register a dataset with keywords
    d_id = _insert_dataset_entry(
        datareg,
        "DESC:datasets:my_first_modify_dataset_with_keywords",
        "0.0.1",
        keywords=["simulation"],
    )

    # Query for the dataset
    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
        return_format="cursorresult",
    )

    # Should only be 1 keyword at this point
    for i, r in enumerate(results):
        assert getattr(r, "dataset.dataset_id") == d_id
        assert getattr(r, "keyword.keyword") == "simulation"
        assert i < 1

    # Add a keyword
    datareg.Registrar.dataset.add_keywords(d_id, ["simulation", "observation"])

    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
        return_format="cursorresult",
    )

    # Should now be two keywords (no duplicates)
    for i, r in enumerate(results):
        assert getattr(r, "dataset.dataset_id") == d_id
        assert getattr(r, "keyword.keyword") in ["simulation", "observation"]
        assert i < 2
