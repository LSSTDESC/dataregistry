import pytest
from dataregistry import DataRegistry
from dataregistry.schema import DEFAULT_NAMESPACE

from database_test_utils import *


def test_register_dataset_with_bad_keywords(dummy_file):
    """
    Make sure we throw exceptions when registering bad keywords

    Case 1) When the keywords aren't strings
    Case 2) When the chosen keyword doesn't exist in the keyword table
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir),
                           namespace=DEFAULT_NAMESPACE)

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
    datareg = DataRegistry(root_dir=str(tmp_root_dir),
                           namespace=DEFAULT_NAMESPACE)

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
    f = datareg.query.gen_filter("keyword.keyword", "==", mykeyword.lower())
    results = datareg.query.find_datasets(
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
    f = datareg.query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
    )

    # Should only be 1 keyword at this point
    assert len(results["dataset.dataset_id"]) == 1
    assert results["dataset.dataset_id"][0] == d_id
    assert results["keyword.keyword"][0] == "simulation"

    # Add a keyword
    datareg.registrar.dataset.add_keywords(d_id, ["simulation", "observation"])

    f = datareg.query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.query.find_datasets(
        ["dataset.dataset_id", "keyword.keyword"],
        [f],
    )

    # Should now be two keywords (no duplicates)
    assert len(results["dataset.dataset_id"]) == 2
    assert results["dataset.dataset_id"][0] == d_id
    assert results["keyword.keyword"][0] in ["simulation", "observation"]


def test_create_custom_keyword(dummy_file):
    """
    Add a keyword to the keyword table.

    Then query to make sure it was added.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        namespace=DEFAULT_NAMESPACE)

    # Add a keyword
    kwd = "test_custom_keyword2"
    datareg.registrar.keyword.create_keywords([kwd])

    all_keywords = datareg.query.get_keyword_list(query_mode="working")

    assert kwd in all_keywords


@pytest.mark.parametrize("custom_keyword", [12, [], None])
def test_create_bad_keyword(dummy_file, custom_keyword):
    """
    Add a bad keyword to the keyword table. Expect failure.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        namespace=DEFAULT_NAMESPACE)

    # Add a keyword
    with pytest.raises(ValueError, match="not a valid keyword string"):
        datareg.registrar.keyword.create_keywords([custom_keyword])


@pytest.fixture(scope="session")
def create_dataset_entry_for_keyword():
    """
    Fixture to create a dataset entry with a keyword.
    """

    def _create_entry(datareg):
        """
        Create a dataset entry.
        """
        d_id = _insert_dataset_entry(datareg,
                                     "DESC:datasets:my_dataset_for_adding_keywords",
                                     "0.0.1")

        return d_id
    return _create_entry


def test_add_keywords_to_dataset(dummy_file, create_dataset_entry_for_keyword):
    """
    Test adding keywords to an existing dataset.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(
        root_dir=str(tmp_root_dir),
        namespace=DEFAULT_NAMESPACE)

    # Register a dataset without keywords
    d_id = create_dataset_entry_for_keyword(datareg)

    # Add keywords to the dataset
    datareg.registrar.keyword.create_keywords(["new_keyword1", "new_keyword2"])
    datareg.registrar.dataset.add_keywords(d_id, ["new_keyword1", "new_keyword2"])

    # Query for the dataset and check keywords
    f = datareg.query.gen_filter("dataset.dataset_id", "==", d_id)
    results = datareg.query.find_datasets(
        property_names=["dataset.dataset_id", "keyword.keyword"],
        filters=[f],
    )

    # Should now be two keywords
    assert len(results["dataset.dataset_id"]) == 2
    assert results["dataset.dataset_id"][0] == d_id
    assert set(results["keyword.keyword"]) == {"new_keyword1", "new_keyword2"}
    assert (datareg.registrar.keyword.get_keywords_from_dataset(d_id) ==
            ["new_keyword1", "new_keyword2"])
