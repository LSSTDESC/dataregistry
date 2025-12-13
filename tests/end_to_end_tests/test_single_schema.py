import pytest
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE
from dataregistry.db_basic import DbConnection

from database_test_utils import *

# This is just to see what backend we are using
# Remember no production schema when using sqlite backend
db_connection = DbConnection(config_file=None, namespace=DEFAULT_NAMESPACE)


@pytest.mark.skipif(
    db_connection._dialect == "sqlite", reason="no production with sqlite"
)
@pytest.mark.parametrize(
    "schema,owner,owner_type",
    [["working", "desc", "group"], ["production", "production", "production"]],
)
def test_register_single_schema(dummy_file, schema, owner, owner_type):
    """
    Usually people connect to the database via a "namespace", which forms a
    connection to both the working and production schemas within that
    namespace.

    Here we test when connecting directly to either the working or production
    schema.

    When you connect directly to a schema you don't have to worry about
    `entry_mode`, you will always write to the schema you specify.
    """

    # Establish connection to database directly to one schema
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir), schema=f"{DEFAULT_NAMESPACE}_{schema}"
    )

    NAME = f"DESC:datasets:test_register_single_schema_{schema}"
    VERSION = "0.0.1"

    # Make a dataset
    d_id = _insert_dataset_entry(
        datareg, NAME, VERSION, owner_type=owner_type, owner=owner
    )

    # Find dataset
    f = datareg.query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.query.find_datasets(
        [
            "dataset.dataset_id",
            "dataset.name",
            "dataset.version_string",
            "dataset.owner_type",
            "dataset.owner",
        ],
        [f],
    )

    assert len(results["dataset.dataset_id"]) == 1
    assert results["dataset.name"][0] == NAME
    assert results["dataset.version_string"][0] == VERSION
    assert results["dataset.owner_type"][0] == owner_type
    assert results["dataset.owner"][0] == owner


@pytest.mark.skipif(
    db_connection._dialect == "sqlite", reason="no production with sqlite"
)
@pytest.mark.parametrize(
    "query_mode", ["working", "production", "both"],
)
def test_query_single_schema_through_namespace(dummy_file, query_mode):
    """
    By default, when connected to a namespace, queries search both the working
    and production schemas and combine the results.

    This behaviour can be changed with `query_mode` to search only the working
    or production schema.

    When both schemas are searched, things like `dataset_id`s are no longer
    assured to be unique when querying (as results can come from two schemas).
    Here we test that when searching only one schema using `query_mode`, we
    only get back single results for a given `dataset_id` (even although we are
    connected via a namespace).
    """

    # Connect to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir), entry_mode="working", query_mode=query_mode
    )
    datareg_prod = DataRegistry(
        root_dir=str(tmp_root_dir), entry_mode="production", query_mode=query_mode
    )

    # Make a dataset in working schema
    d_id = _insert_dataset_entry(
        datareg, f"DESC:query_single_schema_through_namespace_{query_mode}", "0.0.1",
    )

    # Make a dataset in production schema
    d_id_prod = _insert_dataset_entry(
        datareg_prod, f"DESC:query_single_schema_through_namespace_{query_mode}", "0.0.1",
        owner_type="production", owner="production"
    )

    # Find dataset
    if query_mode == "both":
        for DR in [datareg, datareg_prod]:
            f = DR.query.gen_filter("dataset.dataset_id", "==", 1)
            results = DR.query.find_datasets(["dataset.dataset_id",],
                [f],
            )
            assert len(results["dataset.dataset_id"]) == 2
    else:
        for DR in [datareg, datareg_prod]:
            if query_mode == "production":
                f = DR.query.gen_filter("dataset.dataset_id", "==", d_id_prod)
            else:
                f = DR.query.gen_filter("dataset.dataset_id", "==", d_id)
            results = DR.query.find_datasets(["dataset.dataset_id",],
                [f],
            )
            assert len(results["dataset.dataset_id"]) == 1
