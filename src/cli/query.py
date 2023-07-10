from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine
from dataregistry.query import Filter, Query

def dregs_ls(user, dregs_config):
    """
    Parameters
    ----------
    user : str
        User to list dataset entries for
    dregs_config : str
        Path to DREGS config file
    """

    # Establish connection to database
    engine, dialect = create_db_engine(config_file=dregs_config)
    
    # Create query object
    q = Query(engine, dialect, schema_version=SCHEMA_VERSION)

    # Filter on user
    f = Filter("dataset.owner", "==", user)
    f2 = Filter("dataset.owner_type", "==", "user")

    # Query
    results = q.find_datasets(
        ["dataset.name", "dataset.version_string", "dataset.relative_path"], [f,f2]
    )
    
    for r in results:
        print(r)
