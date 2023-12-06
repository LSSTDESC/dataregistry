from dataregistry.registrar_util import (
    _parse_version_string,
    _name_from_relpath,
    _form_dataset_path,
    get_directory_info,
    _read_configuration_file,
)
import os
import pytest


def test_parse_version_string():
    """Make sure version strings are parsed correctly"""

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

    tmp = _form_dataset_path("production", "desc", "my/path", root_dir=None)
    assert tmp == "production/production/my/path"

    tmp = _form_dataset_path("production", "desc", "my/path", root_dir="my/root")
    assert tmp == "my/root/production/production/my/path"

    tmp = _form_dataset_path("group", "desc", "my/path", root_dir=None)
    assert tmp == "group/desc/my/path"

    tmp = _form_dataset_path("user", "desc", "my/path", root_dir="/root/")
    assert tmp == "/root/user/desc/my/path"


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
    """Make sure names are extracted from paths correctly"""

    assert _name_from_relpath("/testing/test") == "test"
    assert _name_from_relpath("./testing/test") == "test"
    assert _name_from_relpath("/testing/test/") == "test"
    assert _name_from_relpath("test") == "test"


def _make_dummy_config(tmpdir, nchars):
    """
    Create a dummy config file in temp directory

    Parameters
    ----------
    tmpdir : py.path.local object (pytest @fixture)
        Temporary directory we can store test config files to
    nchars : int
        Number of characters to put in temp config file

    Returns
    -------
    file_path : str
        Path to temporary config file we can read
    """

    file_path = os.path.join(tmpdir, "dummy_config.txt")

    # Write nchars characters into the dummy file
    with open(file_path, "w") as file:
        for i in range(nchars):
            file.write(f"X")

    return file_path


@pytest.mark.parametrize("nchars,max_config_length,ans", [(10, 10, 10), (100, 10, 10)])
def test_read_file(tmpdir, nchars, max_config_length, ans):
    """Test reading in configuration file, and check truncation warning"""

    # Make sure we warn when truncating
    if nchars > max_config_length:
        with pytest.warns(UserWarning, match="Configuration file is longer"):
            content = _read_configuration_file(
                _make_dummy_config(tmpdir, nchars), max_config_length
            )
        assert len(content) == ans

    # Usual case
    else:
        content = _read_configuration_file(
            _make_dummy_config(tmpdir, nchars), max_config_length
        )
        assert len(content) == ans

    # Make sure we raise an exception when the file doesn't exist
    with pytest.raises(FileNotFoundError, match="not found"):
        _read_configuration_file("i_dont_exist.txt", 10)
