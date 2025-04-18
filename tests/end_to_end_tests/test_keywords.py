import os
import sys

import pandas as pd
import pytest
import sqlalchemy
import yaml
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE
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
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

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

@pytest.mark.parametrize("mykeyword", ["simulation", "SiMuLaTiOn"])
def test_register_dataset_with_keywords(dummy_file, mykeyword):
    """
    Register some basic datasets with keywords.

    Then query the registry on a keyword and make sure we get our datasets back.

    Keywords should also be case insensitive, i.e., all keywords are stored in
    the database as lowercase. Test this also.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Register two datasets with keywords
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:my_first_dataset_with_keyword_{mykeyword}",
        "0.0.1",
        keywords=[mykeyword, "observation"],
    )

    d_id2 = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:my_second_dataset_with_keyword_{mykeyword}",
        "0.0.1",
        keywords=[mykeyword],
    )

    # Query on the "simulation" keyword
    f = datareg.Query.gen_filter("keyword.keyword", "==", mykeyword.lower())
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
        return_format="property_dict",
    )

    # Make sure we get our datasets back
    for tmp_id in [d_id, d_id2]:
        assert tmp_id in results["dataset.dataset_id"]
    for tmp_k in results["keyword.keyword"]:
        assert tmp_k == mykeyword.lower()


def test_modify_dataset_with_keywords(dummy_file):
    """
    Register a basic dataset without any keywords.

    Then add keywords with `add_keywords()`.

    Then query to make sure the keyword was tagged.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

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
    )

    # Should only be 1 keyword at this point
    assert len(results["dataset.dataset_id"]) == 1
    assert results["dataset.dataset_id"][0] == d_id
    assert results["keyword.keyword"][0] == "simulation"

    # Add a keyword
    datareg.Registrar.dataset.add_keywords(d_id, ["simulation", "observation"])

    f = datareg.Query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
    )

    # Should now be two keywords (no duplicates)
    assert len(results["dataset.dataset_id"]) == 2
    assert results["dataset.dataset_id"][0] == d_id
    assert results["keyword.keyword"][0] in ["simulation", "observation"]
