import pytest
import os
from dataregistry.registrar_util import _copy_data


@pytest.fixture
def dummy_file(tmp_path):
    """Create some dummy (temporary) files and directories"""

    # Root temp source dir for files
    tmp_src_dir = tmp_path / "source"
    tmp_src_dir.mkdir()

    # Make a dummy file
    p = tmp_src_dir / "dummy_standalone_file.txt"
    p.write_text("dummy stand alone file")

    # Make a dummy directory with a file in it
    p = tmp_src_dir / "tmpdir" / "tmpdir2"
    p.mkdir(parents=True)
    q = p / "dummy_file_within_folder.txt"
    q.write_text("dummy file within folder")

    # Root temp dest dir to copy into
    tmp_dest_dir = tmp_path / "dest"
    tmp_dest_dir.mkdir()

    return tmp_src_dir, tmp_dest_dir


def test_copy_file(dummy_file):
    """
    Test copying files and directories

    Each test is looped twice, the 2nd emulating overwriting a dataset.
    """

    tmp_src_dir, tmp_dest_dir = dummy_file

    # Copy a single file from source to destination
    for i in range(2):
        _copy_data(
            "file",
            str(tmp_src_dir / "dummy_standalone_file.txt"),
            str(tmp_dest_dir / "dummy_standalone_file.txt"),
        )

        p = tmp_dest_dir / "dummy_standalone_file.txt"
        assert os.path.isfile(p)
        assert p.read_text() == "dummy stand alone file"


def test_copy_directory(dummy_file):
    """
    Test copying files and directories

    Each test is looped twice, the 2nd emulating overwriting a dataset.
    """

    tmp_src_dir, tmp_dest_dir = dummy_file

    # Copy a single directory from source to destination
    for i in range(2):
        _copy_data(
            "directory", str(tmp_src_dir / "tmpdir"), str(tmp_dest_dir / "tmpdir")
        )

        assert os.path.isdir(tmp_dest_dir / "tmpdir")
        assert os.path.isdir(tmp_dest_dir / "tmpdir" / "tmpdir2")
        p = tmp_dest_dir / "tmpdir" / "tmpdir2" / "dummy_file_within_folder.txt"
        assert os.path.isfile(p)
        assert p.read_text() == "dummy file within folder"
