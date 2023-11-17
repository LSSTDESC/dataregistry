import os
import pandas as pd
import sqlalchemy

from dataregistry import DataRegistry

# Establish connection to database (default schema)
datareg = DataRegistry(root_dir="DataRegistry_data")

# Establish connection to database (production schema)
datareg_prod = DataRegistry(
    root_dir="DataRegistry_data_production", schema="production"
)


def test_query_production():
    """Test a query to the production schema"""

    if datareg.db_connection.dialect != "sqlite":
        results = datareg_prod.Query.find_datasets()

        assert results is not None


def test_query_return_format():
    """ Test we get back correct data format from queries """

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
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [],
    )
    assert type(results) == dict


def test_query_all():
    """Test a query where no properties are chosen, i.e., 'return *"""
    results = datareg.Query.find_datasets()

    assert results is not None


def test_query_dataset_cli():
    """Test queries for the dataset table entered from the CLI script"""

    # Query 1: Make sure we find all datasets entered using the CLI
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    assert len(results["dataset.name"]) == 2, "Bad result from query dcli1"


def test_query_dataset():
    """Test queries for the dataset table"""

    # Query 1: Query on dataset name
    f = datareg.Query.gen_filter("dataset.name", "==", "bumped_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    if datareg.Query._dialect != "sqlite":
        for att in results.keys():
            assert len(results[att]) == 4, "Bad result from query d1"

    # Make sure versions (from bump) are correct
    for idx, v in zip(["", "_2", "_3", "_4"], ["0.0.1", "0.0.2", "0.1.0", "1.0.0"]):
        rel_path = f"DESC/datasets/bumped_dataset{idx}"
        m = results["dataset.relative_path"].index(rel_path)
        assert results["dataset.version_string"][m] == v

    # Query 2: Query on owner type
    f = datareg.Query.gen_filter("dataset.owner_type", "!=", "user")
    results = datareg.Query.find_datasets(["dataset.name"], [f])
    assert len(results["dataset.name"]) > 0, f"Bad result from query d2"

    # Query 3: Make sure auto generated name is correct
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/my_first_dataset"
    )
    results = datareg.Query.find_datasets(["dataset.name"], [f])
    if datareg.Query._dialect != "sqlite":
        assert len(results["dataset.name"]) == 1, "Bad result from query d3"
    assert results["dataset.name"][0] == "my_first_dataset", "Bad result from query d3"

    # Query 4: Make sure manual name is correct
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/my_first_named_dataset"
    )
    results = datareg.Query.find_datasets(["dataset.name"], [f])
    if datareg.Query._dialect != "sqlite":
        assert len(results["dataset.name"]) == 1, "Bad result from query d4"
    assert results["dataset.name"][0] == "named_dataset", "Bad result from query d4"

    # Query 5: Query on version suffix
    f = datareg.Query.gen_filter("dataset.version_suffix", "==", "test-suffix")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    if datareg.Query._dialect != "sqlite":
        assert len(results["dataset.name"]) == 2, "Bad result from query d5"

    # Make sure versions (from bump) are correct
    idx = results["dataset.relative_path"].index(
        "DESC/datasets/my_first_suffix_dataset"
    )
    assert results["dataset.version_string"][idx] == "0.0.1"
    idx = results["dataset.relative_path"].index(
        "DESC/datasets/my_first_suffix_dataset_bumped"
    )
    assert results["dataset.version_string"][idx] == "0.1.0"

    # Query 6: Make sure non dummy entries have a non-zero amount of files
    f = datareg.Query.gen_filter("dataset.data_org", "!=", "dummy")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.nfiles", "dataset.total_disk_space"], [f]
    )

    # Make sure nfiles > 0
    for nfiles in results["dataset.nfiles"]:
        assert nfiles > 0

    # Query 7: See if global owner/owner_type allocation worked
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/global_user_dataset"
    )
    results = datareg.Query.find_datasets(["dataset.owner", "dataset.owner_type"], [f])
    for o, ot in zip(results["dataset.owner"], results["dataset.owner_type"]):
        assert o == "DESC group"
        assert ot == "group"

    # Query 8: Make sure dataset gets tagged as overwritten.
    for rel_path in ["file1.txt", "dummy_dir"]:
        f = datareg.Query.gen_filter("dataset.relative_path", "==", rel_path)
        results = datareg.Query.find_datasets(
            [
                "dataset.version_patch",
                "dataset.is_overwritable",
                "dataset.is_overwritten",
            ],
            [f],
        )
        idx = results["dataset.version_patch"].index(1)
        assert results["dataset.is_overwritable"][idx] == True
        assert results["dataset.is_overwritten"][idx] == True
        idx = results["dataset.version_patch"].index(2)
        assert results["dataset.is_overwritable"][idx] == False
        assert results["dataset.is_overwritten"][idx] == False

    # Query 9: Check dataset execution is made correctly
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/execution_test"
    )
    results = datareg.Query.find_datasets(["dataset.execution_id"], [f])

    ex_id = results["dataset.execution_id"][0]
    f = datareg.Query.gen_filter("execution.execution_id", "==", ex_id)
    results = datareg.Query.find_datasets(
        ["execution.name", "execution.description"], [f]
    )

    assert results["execution.name"][0] == "Overwrite execution auto name"
    assert results["execution.description"][0] == "Overwrite execution auto description"

    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/my_first_pipeline_stage1"
    )
    results = datareg.Query.find_datasets(["dataset.dataset_id"], [f])
    input_id = results["dataset.dataset_id"][0]

    f = datareg.Query.gen_filter("dependency.execution_id", "==", ex_id)
    results = datareg.Query.find_datasets(["dependency.input_id"], [f])

    for dep_id in results["dependency.input_id"]:
        assert dep_id == input_id


def test_query_dataset_alias():
    """Test queries of dataset alias table"""

    # Query 1: Query on dataset alias
    f = datareg.Query.gen_filter("dataset_alias.alias", "==", "nice_dataset_name")
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "dataset_alias.dataset_id"], [f]
    )
    if datareg.Query._dialect != "sqlite":
        assert len(results["dataset.dataset_id"]) == 1, "Bad result from query da1"

    # Make sure IDs match up
    for idx in range(len(results["dataset.dataset_id"])):
        assert (
            results["dataset.dataset_id"][idx]
            == results["dataset_alias.dataset_id"][idx]
        )


def test_query_execution():
    """Test queries of execution table"""

    # Query 1: Find the dependencies of an execution
    f = datareg.Query.gen_filter("execution.name", "==", "pipeline_stage_3")
    results = datareg.Query.find_datasets(["execution.execution_id"], [f])
    if datareg.Query._dialect != "sqlite":
        assert len(results["execution.execution_id"]) == 1, "Bad result from query ex1"

    # Find dependencies for this execution
    f = datareg.Query.gen_filter(
        "dependency.execution_id", "==", results["execution.execution_id"][0]
    )
    results = datareg.Query.find_datasets(["dependency.input_id"], [f])
    assert len(results["dependency.input_id"]) == 2, "Bad result from query dep1"


def test_db_version():
    """
    Test if db version is what we think it should be.
    CI makes a fresh database, hence actual db versions should match
    versions to be used when new db is created
    """
    actual_major, actual_minor, actual_patch = datareg.Query.get_db_versioning()
    assert actual_major == 1, "db major version doesn't match expected"
    assert actual_minor == 2, "db minor version doesn't match expected"
    assert actual_patch == 0, "db patch version doesn't match expected"


def test_get_dataset_absolute_path():
    """Test the generation of the full absolute path of a dataset"""

    _TEST_ROOT_DIR = "DataRegistry_data"
    dset_relpath = "DESC/datasets/group1_dataset_1"
    dset_ownertype = "group"
    dset_owner = "group1"
    v = datareg.Query.get_dataset_absolute_path(7)

    assert v == os.path.join(_TEST_ROOT_DIR, dset_ownertype, dset_owner, dset_relpath)
