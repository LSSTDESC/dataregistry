import os

import pytest
import yaml
from dataregistry import DataRegistry

_TEST_ROOT_DIR = "DataRegistry_data"
_NERSC_SITE = "/global/cfs/cdirs/lsst/utilities/data-registry"
_ENV_SITE = "nersc"


@pytest.mark.parametrize(
    "root_dir,site,set_env_var,ans",
    [
        (_TEST_ROOT_DIR, None, False, _TEST_ROOT_DIR),
        (None, "nersc", False, _NERSC_SITE),
        (None, None, True, _NERSC_SITE),
        (None, None, False, _NERSC_SITE),
        (_TEST_ROOT_DIR, "nersc", False, _TEST_ROOT_DIR),
        (_TEST_ROOT_DIR, "nersc", True, _TEST_ROOT_DIR),
        (_TEST_ROOT_DIR, None, True, _TEST_ROOT_DIR),
    ],
)
def test_root_dir_manual(root_dir, site, set_env_var, ans):
    """
    Test various ways of passing the root_dir

    - Manually by passing `root_dir` to `DataRegistry`
    - Through a "site" reference string
    - Through setting the $DATAREG_SITE environment variable
    - The default (passing None), which should give the NERSC site
    """

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

    assert reg.root_dir == ans
