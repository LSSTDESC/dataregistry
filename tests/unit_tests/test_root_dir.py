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
        (_TEST_ROOT_DIR, None, False, _TEST_ROOT_DIR),
        (None, "nersc", False, _NERSC_SITE),
        (None, None, True, _ENV_SITE),
        (None, None, False, _NERSC_SITE),
        (_TEST_ROOT_DIR, "nersc", False, _TEST_ROOT_DIR),
        (_TEST_ROOT_DIR, "nersc", True, _TEST_ROOT_DIR),
        (_TEST_ROOT_DIR, None, True, _TEST_ROOT_DIR),
    ],
)
def test_root_dir_manual(root_dir, site, set_env_var, ans):
    """Test various ways of passing the root_dir"""

    # Case where we are using the DATAREG_SITE env variable
    if set_env_var:
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
