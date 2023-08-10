from dataregistry.db_basic import ownertypeenum
from dataregistry.registrar_util import (
    _parse_version_string,
    _name_from_relpath,
    _form_dataset_path,
    get_directory_info,
)
import os


def test_parse_version_string():
    """ Make sure version strings are parsed correctly """

    # Test case with no version suffix
    tmp = _parse_version_string("1.2.3")
    assert type(tmp) == dict
    assert len(tmp.keys()) == 3
    assert tmp["major"] == "1"
    assert tmp["minor"] == "2"
    assert tmp["patch"] == "3"

    # Test case with version suffix
    tmp = _parse_version_string("4.5.6.mysuffix", with_suffix=True)
    assert type(tmp) == dict
    assert len(tmp.keys()) == 4
    assert tmp["major"] == "4"
    assert tmp["minor"] == "5"
    assert tmp["patch"] == "6"
    assert tmp["suffix"] == "mysuffix"

    # Test case with no version suffix but with_suffix=True
    tmp = _parse_version_string("7.8.9", with_suffix=True)
    assert type(tmp) == dict
    assert len(tmp.keys()) == 3
    assert tmp["major"] == "7"
    assert tmp["minor"] == "8"
    assert tmp["patch"] == "9"


def test_form_dataset_path():
    """
    Test dataset path construction

    Datasets should come back with the format:
        <root_dir>/<owner_type>/<owner>/<relative_path>
    """

    tmp = _form_dataset_path(
        "sql_lite", "my_schema", "production", "desc", "my/path", root_dir=None
    )
    assert tmp == "sql_lite/my_schema/production/production/my/path"

    tmp = _form_dataset_path(
        "postgres", "registry", "production", "desc", "my/path", root_dir="my/root"
    )
    assert tmp == "my/root/postgres/registry/production/production/my/path"

    tmp = _form_dataset_path(
        "postgres", "registry", "group", "desc", "my/path", root_dir=None
    )
    assert tmp == "postgres/registry/group/desc/my/path"

    tmp = _form_dataset_path(
        "postgres", "registry", "user", "desc", "my/path", root_dir="/root/"
    )
    assert tmp == "/root/postgres/registry/user/desc/my/path"


def test_directory_info():
    """
    Test getting number of files and disk space usage from a directory.

    Cant assert any specific numbers without hard coding, so just make sure we
    get a result from the function.
    """
    num_files, total_size = get_directory_info(".")
    assert num_files > 0
    assert total_size > 0


def test_name_from_relpath():
    """ Make sure names are exctracted from paths correctly """

    assert _name_from_relpath("/testing/test") == "test"
    assert _name_from_relpath("./testing/test") == "test"
    assert _name_from_relpath("/testing/test/") == "test"
    assert _name_from_relpath("test") == "test"
