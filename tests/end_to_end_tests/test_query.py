import os

from dataregistry import DataRegistry

# Establish connection to database
datareg = DataRegistry(root_dir="DataRegistry_data")


def test_query_dataset_cli():
    """ Test queries for the dataset table entered from the CLI script """

    # Query 1: Make sure we find all datasets entered using the CLI
    f = datareg.Query.gen_filter("dataset.name", "==", "my_cli_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    assert results.rowcount == 2, "Bad result from query dcli1"


def test_query_dataset():
    """ Test queries for the dataset table """

    # Query 1: Query on dataset name
    f = datareg.Query.gen_filter("dataset.name", "==", "bumped_dataset")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    assert results.rowcount == 4, "Bad result from query d1"

    # Make sure versions (from bump) are correct
    for r in results:
        if r.relative_path == "DESC/datasets/bumped_dataset":
            assert r.version_string == "0.0.1"
        elif r.relative_path == "DESC/datasets/bumped_dataset_2":
            assert r.version_string == "0.0.2"
        elif r.relative_path == "DESC/datasets/bumped_dataset_3":
            assert r.version_string == "0.1.0"
        elif r.relative_path == "DESC/datasets/bumped_dataset_4":
            assert r.version_string == "1.0.0"

    # Query 2: Query on owner type
    f = datareg.Query.gen_filter("dataset.owner_type", "!=", "user")
    results = datareg.Query.find_datasets(["dataset.name"], [f])
    assert results.rowcount > 0, f"Bad result from query d2 ({results.rowcount})"

    # Query 3: Make sure auto generated name is correct
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/my_first_dataset"
    )
    results = datareg.Query.find_datasets(["dataset.name"], [f])
    assert results.rowcount == 1, "Bad result from query d3"
    for r in results:
        assert r.name == "my_first_dataset", "Bad result from query d3"

    # Query 4: Make sure manual name is correct
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/my_first_named_dataset"
    )
    results = datareg.Query.find_datasets(["dataset.name"], [f])
    assert results.rowcount == 1, "Bad result from query d4"
    for r in results:
        assert r.name == "named_dataset", "Bad result from query d4"

    # Query 5: Query on version suffix
    f = datareg.Query.gen_filter("dataset.version_suffix", "==", "test-suffix")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f]
    )
    assert results.rowcount == 2, "Bad result from query d5"

    # Make sure versions (from bump) are correct
    for r in results:
        if r.relative_path == "DESC/datasets/my_first_suffix_dataset":
            assert r.version_string == "0.0.1"
        elif r.relative_path == "DESC/datasets/my_first_suffix_dataset_bumped":
            assert r.version_string == "0.1.0"

    # Query 6: Make sure non dummy entries have a non-zero amount of files
    f = datareg.Query.gen_filter("dataset.data_org", "!=", "dummy")
    results = datareg.Query.find_datasets(
        ["dataset.name", "dataset.nfiles", "dataset.total_disk_space"], [f]
    )

    # Make sure nfiles > 0
    for r in results:
        assert r.nfiles > 0, "Bad result from query d6"

    # Query 7: See if global owner/owner_type allocation worked
    f = datareg.Query.gen_filter(
        "dataset.relative_path", "==", "DESC/datasets/global_user_dataset"
    )
    results = datareg.Query.find_datasets(["dataset.owner", "dataset.owner_type"], [f])
    for r in results:
        assert r.owner == "DESC group"
        assert r.owner_type == "group"


def test_query_dataset_alias():
    """ Test queries of dataset alias table """

    # Query 1: Query on dataset alias
    f = datareg.Query.gen_filter("dataset_alias.alias", "==", "nice_dataset_name")
    results = datareg.Query.find_datasets(
        ["dataset.dataset_id", "dataset_alias.dataset_id"], [f]
    )
    assert results.rowcount == 1, "Bad result from query da1"

    # Make sure IDs match up
    for r in results:
        assert r[0] == r[1]


def test_query_execution():
    """ Test queries of execution table """

    # Query 1: Find the dependencies of an execution
    f = datareg.Query.gen_filter("execution.name", "==", "pipeline_stage_3")
    results = datareg.Query.find_datasets(["execution.execution_id"], [f])
    assert results.rowcount == 1, "Bad result from query ex1"

    # Find dependencies for this execution
    f = datareg.Query.gen_filter("dependency.execution_id", "==", next(results)[0])
    results = datareg.Query.find_datasets(["dependency.input_id"], [f])
    assert results.rowcount == 2, "Bad result from query dep1"


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
