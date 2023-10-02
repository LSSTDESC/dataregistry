from dataregistry.db_basic import DbConnection
import os


def test_connect_manual():
    """Test connection to database with manually passed config location"""
    conn = DbConnection(
        config_file=os.path.join(os.getenv("HOME"), ".config_reg_access"), verbose=True
    )
    assert conn.engine is not None
    assert conn.dialect is not None
    if conn.dialect != "sqlite":
        assert conn.schema is not None


def test_connect_env():
    """Test connection to database with DATAREG_CONFIG env set"""
    os.environ["DATAREG_CONFIG"] = os.path.join(os.getenv("HOME"), ".config_reg_access")
    conn = DbConnection(verbose=True)
    assert conn.engine is not None
    assert conn.dialect is not None
    if conn.dialect != "sqlite":
        assert conn.schema is not None


def test_connect_default():
    """Test connection to database with default config location"""
    conn = DbConnection(verbose=True)
    assert conn.engine is not None
    assert conn.dialect is not None
    if conn.dialect != "sqlite":
        assert conn.schema is not None
