from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine
from dataregistry.db_basic import DbConnection
import os


def test_connect_manual():
    """Test connection to database with manually passed config location"""
    engine, dialect = create_db_engine(
        config_file=os.path.join(os.getenv("HOME"), ".config_reg_access"), verbose=True
    )
    assert engine is not None
    assert dialect is not None


def test_connect_env():
    """Test connection to database with DATAREG_CONFIG env set"""
    os.environ["DATAREG_CONFIG"] = os.path.join(os.getenv("HOME"), ".config_reg_access")
    engine, dialect = create_db_engine(verbose=True)
    assert engine is not None
    assert dialect is not None


def test_connect_default():
    """Test connection to database with default config location"""
    engine, dialect = create_db_engine(verbose=True)
    assert engine is not None
    assert dialect is not None

def test_connection_class():
    """
    Test connection to database with default config location,
    making use of class DbConnection
    """
    db_connection = DbConnection(None, verbose=True)
    assert db_connection.engine is not None
    assert db_connection.dialect is not None
    if db_connection.dialect != 'sqlite':
        assert db_connection.schema is not None
