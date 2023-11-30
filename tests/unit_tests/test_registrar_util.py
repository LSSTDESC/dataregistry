import pytest
from dataregistry.registrar_util import (
    _parse_version_string,
    _name_from_relpath,
    _form_dataset_path,
    get_directory_info,
)
import os


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


@pytest.mark.parametrize(
    "rel_path,ans",
    [
        ("/testing/test", "test"),
        ("./testing/test", "test"),
        ("/testing/test/", "test"),
        ("test", "test"),
    ],
)
def test_name_from_relpath(rel_path, ans):
    """Make sure names are exctracted from paths correctly"""

    assert _name_from_relpath(rel_path) == ans
