import os

import pytest
from dataregistry.registrar.registrar_util import (
    _form_dataset_path,
    _relpath_from_name,
    _parse_version_string,
    _read_configuration_file,
    get_directory_info,
)


@pytest.mark.parametrize(
    "v_str,v_len,ans_maj,ans_min,ans_ptc,ans_suf,w_suf,bad",
    [
        ("1.2.3", 3, "1", "2", "3", None, False, False),
        ("4.5.6.mysuffix", 4, "4", "5", "6", "mysuffix", True, False),
        ("7.8.9", 3, "7", "8", "9", None, True, False),
        ("1.2.3.4.5", 5, "1", "2", "3", None, False, True),
        ("1.2.3.mysuffix", 4, "1", "2", "3", "mysuffix", False, True),
        ("-1.2.3", 3, "-1", "2", "3", None, False, True),
    ],
)
def test_parse_version_string(
    v_str, v_len, ans_maj, ans_min, ans_ptc, ans_suf, w_suf, bad
):
    """Make sure version strings are parsed correctly"""

    # Test bad cases, should raise ValueError
    if bad:
        with pytest.raises(ValueError):
            tmp = _parse_version_string(v_str, with_suffix=w_suf)

    # Test good cases
    else:
        tmp = _parse_version_string(v_str, with_suffix=w_suf)
        assert type(tmp) == dict
        assert len(tmp.keys()) == v_len
        assert tmp["major"] == ans_maj
        assert tmp["minor"] == ans_min
        assert tmp["patch"] == ans_ptc
        if v_len == 4:
            assert tmp["suffix"] == ans_suf


@pytest.mark.parametrize(
    "owner_type,owner,rel_path,root_dir,ans",
    [
        ("production", "desc", "my/path", None, "production/production/my/path"),
        (
            "production",
            "desc",
            "my/path",
            "my/root",
            "my/root/production/production/my/path",
        ),
        ("group", "desc", "my/path", None, "group/desc/my/path"),
        ("user", "desc", "my/path", "/root/", "/root/user/desc/my/path"),
    ],
)
def test_form_dataset_path(owner_type, owner, rel_path, root_dir, ans):
    """
    Test dataset path construction

    Datasets should come back with the format:
        <root_dir>/<owner_type>/<owner>/<relative_path>
    """

    tmp = _form_dataset_path(owner_type, owner, rel_path, root_dir=root_dir)
    assert tmp == ans


def test_directory_info():
    """
    Test getting number of files and disk space usage from a directory.

    Cant assert any specific numbers without hard coding, so just make sure we
    get a result from the function.
    """
    num_files, total_size = get_directory_info(".")
    assert num_files > 0
    assert total_size > 0


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

@pytest.mark.parametrize(
    "name,version_string,version_suffix,ans",
    [
        ("mydataset", "1.1.1", None, "mydataset_1.1.1"),
        ("mydataset", "1.1.1", "v1", "mydataset_1.1.1_v1"),
    ],
)
def test_relpath_from_name(name, version_string, version_suffix, ans):
    """
    Test dataset path construction

    Datasets should come back with the format:
        <root_dir>/<owner_type>/<owner>/<relative_path>
    """

    tmp = _relpath_from_name(name, version_string, version_suffix)
    assert tmp == ans
