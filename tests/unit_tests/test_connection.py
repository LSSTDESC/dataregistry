import os

import pytest
from dataregistry.db_basic import DbConnection

# This is always assumed to exist from the one-time-setup for the dataregistry
_DEFAULT_LOCATION = os.path.join(os.getenv("HOME"), ".config_reg_access")


@pytest.mark.parametrize(
    "config_file,set_env_var",
    [
        (_DEFAULT_LOCATION, False),
        (None, False),
        (None, True),
    ],
)
def test_connection(config_file, set_env_var):
    """
    Test various ways of passing the connection config file

    - Manually by passing `config_file` to `DataRegistry`
    - Through setting the $DATAREG_CONFIG environment variable
    - The default (passing None), which should look for $HOME/.config_reg_access
    """

    # Case where we are using the DATAREG_SITE env variable
    if set_env_var:
        os.environ["DATAREG_CONFIG"] = _DEFAULT_LOCATION
    else:
        if os.environ.get("DATAREG_CONFIG"):
            os.environ.pop("DATAREG_CONFIG")

    conn = DbConnection(config_file=config_file, verbose=True)
    assert conn.engine is not None
    assert conn.dialect is not None
    if conn.dialect != "sqlite":
        assert conn.schema is not None
