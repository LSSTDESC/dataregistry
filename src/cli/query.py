import os
from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine
from dataregistry.query import Filter, Query


def dregs_ls(owner, owner_type, show_all, dregs_config):
    """
    Queries the data registry for datasets, displaying their relative paths.

    Can apply a "owner" and/or "owner_type" filter.

    Parameters
    ----------
    owner : str
        Owner to list dataset entries for
    owner_type : str
        Owner type to list dataset entries for
    show_all : bool
        True to show all datasets, no filters
    dregs_config : str
        Path to DREGS config file
    """

    # Establish connection to database
    engine, dialect = create_db_engine(config_file=dregs_config)

    # Create query object
    q = Query(engine, dialect, schema_version=SCHEMA_VERSION)

    # Filter on dataset owner and/or owner_type
    filters = []

    print("\nDREGS query:",end=" ")
    if not show_all:
        if owner_type is not None:
            filters.append(Filter("dataset.owner_type", "==", owner_type))
            print(f"owner_type=={owner_type}",end=" ")

        if owner is None:
            if owner_type is None:
                filters.append(Filter("dataset.owner", "==", os.getenv("USER")))
                print(f"owner=={os.getenv('USER')}",end=" ")
        else:
            filters.append(Filter("dataset.owner", "==", owner))
            print(f"owner=={owner}",end=" ")
    else:
        print("all dataset", end=" ")

    # Query
    results = q.find_datasets(
        [
            "dataset.name",
            "dataset.version_string",
            "dataset.relative_path",
            "dataset.owner",
            "dataset.owner_type",
        ],
        filters,
    )

    print(f"({results.rowcount} results)")

    # Loop over each result and print.
    if results.rowcount > 0:
        print("Format : '[owner_type:owner] name : version_string -> relative_path'")
        for r in results:
            print(
                f" - [{r.owner_type}:{r.owner}] {r.name} :",
                f"v{r.version_string} -> {r.relative_path}",
            )
