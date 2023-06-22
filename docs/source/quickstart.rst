Quickstart
==========

A quick example to get started

Entering your first dataset
---------------------------

Making your first query
-----------------------

.. code-block:: python

   from dataregistry.query import Query, Filter
   from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

   # Establish connection to database
   engine, dialect = create_db_engine(config_file=DREGS_CONFIG)
   
   # Create query object
   q = Query(engine, dialect, schema_version=SCHEMA_VERSION)

   # Query 1: Query dataset name
   f = Filter('dataset.name', '==', 'DESC dataset 1')
   results = q.find_datasets(['dataset.dataset_id', 'dataset.name'], [f])
