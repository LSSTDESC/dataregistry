import os

import pytest
from dataregistry.db_basic import DbConnection

# This is the old default. It's used when not runniing at NERSC
# NERSC default location is in NERSC /global/common so can't be tested here
_OLD_DEFAULT_LOCATION = os.path.join(os.getenv("HOME"), ".config_reg_access")


@pytest.mark.parametrize(
    "config_file,set_env_var",
    [
        (_OLD_DEFAULT_LOCATION, False),
        (None, True),
        (None, False),
    ],
)
def test_connection(config_file, set_env_var):
    """
    Test various ways of passing the connection config file

    - Manually by passing `config_file` to `DataRegistry`
    - Through setting the $DATAREG_CONFIG environment variable
    - Using (non-NERSC) default
    Cannot test using the NERSC default here because the default is now a
    file at NERSC
    """

    # Case where we are using the DATAREG_SITE env variable
    if set_env_var:
        os.environ["DATAREG_CONFIG"] = _OLD_DEFAULT_LOCATION
    else:
        if os.environ.get("DATAREG_CONFIG"):
            os.environ.pop("DATAREG_CONFIG")

    conn = DbConnection(config_file=config_file)
    assert conn.engine is not None
    assert conn.dialect is not None
    if conn.dialect != "sqlite":
        assert conn.schema is not None
