from dataregistry import DataRegistry
import os
import pytest
import yaml

_TEST_ROOT_DIR = "DataRegistry_data"
_NERSC_SITE = "/global/cfs/cdirs/desc-co/registry-beta"
_ENV_SITE = "/test/dir"


@pytest.mark.parametrize(
    "root_dir,site,ans",
    [
        (_TEST_ROOT_DIR, None, _TEST_ROOT_DIR),
        (None, "nersc", _NERSC_SITE),
        (None, None, _ENV_SITE),
        (None, None, _NERSC_SITE),
        (_TEST_ROOT_DIR, "nersc", _TEST_ROOT_DIR),
    ],
)
def test_root_dir_manual(root_dir, site, ans):
    """Test various ways of passing the root_dir"""

    # Case where we are using the DATAREG_SITE env variable
    if ans == _ENV_SITE:
        os.environ["DATAREG_SITE"] = _ENV_SITE
    else:
        if os.environ.get("DATAREG_SITE"):
            os.environ.pop("DATAREG_SITE")

    # Connect to registry
    reg = DataRegistry(root_dir=root_dir, site=site)

    # Test
    assert reg.db_connection.engine is not None
    assert reg.db_connection.dialect is not None
    if reg.db_connection.dialect != "sqlite":
        assert reg.db_connection.schema is not None

    assert reg.Registrar._root_dir == ans
    assert reg.Query._root_dir == ans
