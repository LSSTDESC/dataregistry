import os
from dataregistry.db_basic import DbConnection
from dataregistry.query import Filter, Query


def dregs_ls(owner, owner_type, show_all, config, schema):
    """
    Queries the data registry for datasets, displaying their relative paths.

    Can apply a "owner" and/or "owner_type" filter.

    Note that the production schema will always be searched against, even if it
    is not the passed `schema`.

    Parameters
    ----------
    owner : str
        Owner to list dataset entries for
    owner_type : str
        Owner type to list dataset entries for
    show_all : bool
        True to show all datasets, no filters
    config : str
        Path to data registry config file
    schema : str
        Which schema to search
    """

    # Establish connection to the regular schema
    connection = DbConnection(config, schema=schema)
    # Create query object
    q = Query(connection)

    # Establish connection to the production schema
    if connection.schema != "production":
        connection_prod = DbConnection(config, schema="production")
        # Create production query object
        q_prod = Query(connection_prod)
    else:
        connection_prod = None
        q_prod = None

    # Filter on dataset owner and/or owner_type
    filters = []

    print("\nDataRegistry query:", end=" ")
    if not show_all:
        if owner_type is not None:
            filters.append(Filter("dataset.owner_type", "==", owner_type))
            print(f"owner_type=={owner_type}", end=" ")

        if owner is None:
            if owner_type is None:
                filters.append(Filter("dataset.owner", "==", os.getenv("USER")))
                print(f"owner=={os.getenv('USER')}", end=" ")
        else:
            filters.append(Filter("dataset.owner", "==", owner))
            print(f"owner=={owner}", end=" ")
    else:
        print("all datasets", end=" ")

    # Show the format out the output.
    mystr = "Format : '[owner_type:owner] name : version_string -> relative_path'"
    print(f"\n{mystr}")
    print("-" * len(mystr))

    # Loop over this schema and the production schema and print the results
    for this_q, this_connection in zip([q, q_prod], [connection, connection_prod]):
        if this_q is None:
            continue

        # Query
        results = this_q.find_datasets(
            [
                "dataset.name",
                "dataset.version_string",
                "dataset.relative_path",
                "dataset.owner",
                "dataset.owner_type",
            ],
            filters,
        )

        # Loop over each result and print.
        if results.rowcount > 0:
            for r in results:
                print(
                    f" - [{r.owner_type}:{r.owner}] {r.name} :",
                    f"v{r.version_string} -> {r.relative_path}",
                )
