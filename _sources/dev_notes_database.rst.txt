Database structure
==================

The database schemas
--------------------

There are two primary database schemas which the majority of users will work with:

- The "default" schema, which the a hard-coded variable ``DEFAULT_SCHEMA_WORKING`` in
  the ``src/dataregistry/db_basic.py`` file. It can be imported by ``from
  dataregistry.db_basic import DEFAULT_SCHEMA_WORKING``
- The production schema. This is where production datasets go, and has only
  read access for the general user. By default this schema is named
  "production", however during schema creation (see below) you can specify the
  name of the production schema (though this should only be changed for testing
  purposes).

Users can specify their own schemas during the initialization of the
``DataRegistry`` object (by default ``DEFAULT_SCHEMA_WORKING`` is connected to). If
they wish to connect to the production schema its name will have to be manually
entered (see production schema tutorial). If the user wishes to connect to a
custom schema they will have to manually enter its name, however they will have
to have created their own schema for it to work.

When using *SQLite* as the backend (useful for testing), the concepts of
schemas do not exist.

First time creation of database schemas
---------------------------------------

In the top level ``scripts`` directory there is a ``create_registry_schema.py``
script to do the initial schema creation. Before using the data registry, both
for *Postgres* and *SQLite* backends, this script must have been run.

First, make sure your ``~/.config_reg_access`` and ``~/.pgpass`` are correctly
setup (see "Getting set up" for more information on these configuration files).
When creating schemas at NERSC, make sure the SPIN instance of the *Postgres*
database is running.

The script must be run twice, first for the production schema, then for the
general schema (or run in a single call when using the ``--create_both``
argument).  There are three arguments that can be specified (all optional):

- ``--config`` : Location of the data registry configuration file
  (``~/.config_reg_access`` by default)
- ``--schema`` : The name of the schema (default is ``DEFAULT_SCHEMA_WORKING``)
- ``--production-schema``: The name of the production schema (default
  "production")
- ``--create_both`` : Create both the production schema and working schema in
  one call (the production schema will be made first, then the working schema)

The typical initlalization would be:

.. code-block:: bash
   
   python3 create_registry_schema.py --create_both
