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


def test_bad_register_dataset_external(dummy_file):
    """Register an external dataset without providing a url or contact_email"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Add entry
    with pytest.raises(ValueError, match="require either a url or contact_email"):
        d_id = _insert_dataset_entry(
            datareg,
            "DESC:datasets:my_first_external_dataset",
            "0.0.1",
            location_type="external",
        )


@pytest.mark.parametrize(
    "contact_email,url,rel_path",
    [
        (None, "www.info.com", "external_with_url"),
        ("email@emai.com", None, "external_with_email"),
        ("email@emai.com", "www.info.com", "external_with_email_and_url"),
    ],
)
def test_register_dataset_external(dummy_file, contact_email, url, rel_path):
    """Register an external dataset"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    # Add entry
    d_id = _insert_dataset_entry(
        datareg,
        f"DESC:datasets:{rel_path}",
        "0.0.1",
        location_type="external",
        contact_email=contact_email,
        url=url,
    )

    # Query
    f = datareg.query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.query.find_datasets(
        [
            "dataset.contact_email",
            "dataset.url",
        ],
        [f],
    )

    assert len(results["dataset.contact_email"]) == 1
    assert results["dataset.contact_email"][0] == contact_email
    assert results["dataset.url"][0] == url
