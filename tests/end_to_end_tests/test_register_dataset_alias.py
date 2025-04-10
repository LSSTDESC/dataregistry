from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE

from database_test_utils import *
import pytest

@pytest.mark.parametrize(
    "query_mode",
    [
        ("both"),
        ("production")
    ]
)
def test_register_dataset_alias(dummy_file, query_mode):
    """Register a dataset and make a dataset alias entry for it"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE, query_mode=query_mode)

    # Add two dataset
    d_id = _insert_dataset_entry(
        datareg,
        f"alias_test_entry_{query_mode}",
        "0.0.1",
    )

    d2_id = _insert_dataset_entry(
        datareg,
        f"alias_test_entry_2_{query_mode}",
        "0.0.2",
    )

    # Add alias
    a_id = _insert_alias_entry(datareg.Registrar, "nice_dataset_name", d_id)

    # Query
    f = datareg.Query.gen_filter("dataset_alias.alias", "==", "nice_dataset_name")
    results = datareg.Query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset_alias.dataset_id",
        ],
        [f],
    )

    assert len(results["dataset_alias.dataset_id"]) == 1
    assert results["dataset.dataset_id"][0] == d_id
    assert results["dataset_alias.dataset_id"][0] == d_id

    # Try to reuse alias without supersede.  Should fail
    a2_id = _insert_alias_entry(datareg.Registrar, "nice_dataset_name", d2_id)
    assert a2_id is None

    # Try again with supersede
    a2_id = _insert_alias_entry(
        datareg.Registrar, "nice_dataset_name", d2_id, supersede=True
    )
    assert a2_id is not None

    # Check that old entry with this alias has been marked as superseded
    f = datareg.Query.gen_filter("dataset_alias.dataset_alias_id", "==", a_id)
    results = datareg.Query.find_aliases(
        property_names=["dataset_alias.supersede_date"], filters=[f]
    )
    assert results["dataset_alias.supersede_date"][0] is not None

    # Add an alias to the alias
    aa_id = _insert_alias_entry(datareg.Registrar, "alias_to_alias", None, a2_id)
    id, ref_type = datareg.Query.resolve_alias("alias_to_alias")
    assert id == a2_id
    assert ref_type == "alias"

    # Fully resolve
    dataset_id = datareg.Query.resolve_alias_fully("alias_to_alias")
    assert dataset_id == d2_id
    assert aa_id is not None
