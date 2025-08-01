import os

import pytest
from dataregistry import DataRegistry, DbConnection
from dataregistry.schema import DEFAULT_NAMESPACE
from dataregistry.db_basic import _insert_keyword

from database_test_utils import *
from sqlalchemy.exc import IntegrityError

# This is just to see what backend we are using
# Remember no production schema when using sqlite backend
db_connection = DbConnection(config_file=None, namespace=DEFAULT_NAMESPACE)

@pytest.mark.skipif(
    db_connection.dialect == "sqlite", reason="no production with sqlite"
)
@pytest.mark.parametrize("schema", ["production", "working"])
def test_get_dataset_absolute_path(dummy_file, schema):
    """
    Test the generation of the full absolute path of a dataset using the
    `Query.get_dataset_absolute_path()` function
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE,
            entry_mode=schema, query_mode=schema)

    dset_name = f"DESC:datasets:get_dataset_absolute_path_test_{schema}"
    dset_ownertype = "group" if schema == "working" else "production"
    dset_owner = "group1" if schema == "working" else "production"
    dset_relpath = "my/path"
    old_loc = os.path.join(tmp_src_dir, 'file1.txt')


    # Make a basic entry
    d_id_1 = _insert_dataset_entry(
        datareg,
        dset_name,
        "0.0.1",
        owner_type=dset_ownertype,
        owner=dset_owner,
        relative_path=dset_relpath,
        old_location=old_loc,
    )

    v = datareg.Query.get_dataset_absolute_path(d_id_1, schema=schema)

    if datareg.Query._dialect == "sqlite":
        assert v == os.path.join(
            str(tmp_root_dir), dset_ownertype, dset_owner, dset_relpath
        )
    # Cannot easily make a similar test for non-sqlite since schema type
    # ("production", "working") is not what's used to form the path; the
    # full schema name is.

    # See if the file is actually there
    assert os.path.exists(v)

def test_find_entry(dummy_file):
    """
    Test the `find_entry()` function.

    First create a dataset/execution/alias entry, then make sure we can find it
    using the generic `find_entry` function in the `BaseTable` class.
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)
    # Make a dataset
    d_id = _insert_dataset_entry(datareg, "test_find_entry:dataset", "0.0.1")

    # Find it
    r = datareg.Registrar.dataset.find_entry(d_id)
    assert r is not None
    assert r.dataset_id == d_id
    assert r.name == "test_find_entry:dataset"
    assert r.version_string == "0.0.1"

    # Make an execution
    e_id = _insert_execution_entry(datareg, "test_find_entry_execution", "an execution")

    # Find it
    r = datareg.Registrar.execution.find_entry(e_id)
    assert r is not None
    assert r.description == "an execution"
    assert r.name == "test_find_entry_execution"

    # Make a dataset alias
    da_id = _insert_alias_entry(datareg.Registrar, "test_find_entry_alias", d_id)

    # Find it
    r = datareg.Registrar.dataset_alias.find_entry(da_id)
    assert r is not None
    assert r.alias == "test_find_entry_alias"
    assert r.dataset_id == d_id


def test_get_modifiable_columns(dummy_file):
    """Test the `get_modifiable_columns()` function"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    mod_list = datareg.Registrar.dataset.get_modifiable_columns()
    assert "description" in mod_list

    mod_list = datareg.Registrar.execution.get_modifiable_columns()
    assert "description" in mod_list


def test_get_keywords(dummy_file):
    """Test the `get_keywords()` function"""

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE,
            query_mode="working")

    keywords = datareg.Query.get_keyword_list()

    assert "simulation" in keywords
    assert "observation" in keywords

@pytest.mark.parametrize("mykeyword", ["KeYwOrD", "anotherkeyword"])
def test_insert_keywords(dummy_file, mykeyword):
    """
    Test inserting a keyword with `_insert_keyword`


    Make sure case sensitivity is ignored (keywords are always entered as lower case
    """

    # Establish connection to database
    tmp_src_dir, tmp_root_dir = dummy_file
    datareg = DataRegistry(root_dir=str(tmp_root_dir), namespace=DEFAULT_NAMESPACE)

    _insert_keyword(datareg.db_connection, mykeyword, True)

    # Second time should fail, keywords are unique
    if datareg.db_connection._dialect == "sqlite":
        with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
            _insert_keyword(datareg.db_connection, mykeyword, True)
    else:
        with pytest.raises(IntegrityError, match="duplicate key value"):
            _insert_keyword(datareg.db_connection, mykeyword, True)
