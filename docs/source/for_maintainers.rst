.. _maintenance

Maintenance
===========

Procedure for Database Upgrades
-------------------------------

Creating an alternate database
******************************

If it doesn't already exist, create an alternate database from ``psql`` using
the admin account:

.. code-block:: bash

   $ psql dest_data_registry reg_admin
   desc_data_registry=# create database alt_db;

and add an entry to your .pgpass for the reg_admin account and alt_db database.
If you expect to use standard ``dataregistry`` utilities for your updates
(recommended) you'll also need an alternate config file for connecting to
the alternate database as reg_admin.

Dump the active database
************************

Dump both schemas and data from the ``desc_data_registry`` database
.. code-block:: bash

   $ pg_dump -U reg_admin desc_data_registry --schema=lsst_desc_production --file=production_dump.sql
   $ pg_dump -U reg_admin desc_data_registry --schema=lsst_desc_working --file=working_dump.sql

See ``pg_dump`` documentation for a description of all options.  For example you
might want to use a different format than the default simple one used here.

Restore to alt_db database
**************************

.. code-block:: bash

   $ psql -U reg_admin -X --set ON_ERROR_STOP=on alt_db < production_dump.sql
   $ psql -U reg_admin -X --set ON_ERROR_STOP=on alt_db < working_dump.sql

Depending on the format of your dumped data, you might instead need to use the
``pg_restore`` program rather than ``psql``  to do the restore.

Test and apply for real
***********************

If the update involves changes to the schema you'll need a script to implement
them and also add an entry to the ``provenance`` table. You'll also need
to update the database version as stored in
`dataregistry/src/dataregistry/schema/schema_version.py`.
If the update involves changes to entries stored in the database, you'll need
a script for that as well (or if more convenient use a single script for both).
See examples in the dataregistry GitHub repo under
`dataregistry/scripts/schema_migration/`.

Run your script(s) in alt_db, fix any issues, then run in the
``desc_data_registry``. Once you're satisfied everything is ok,
delete the copy of the schemas in ``alt_db``
