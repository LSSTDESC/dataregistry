The ``dataregistry`` package
============================

Reference documentation for the core objects within the ``dataregistry``
package.  Demonstrations of their usage can be found in the :ref:`tutorials section <tutorials-python>`.

.. _dataregistry_class:

The DataRegistry class
----------------------

The ``DataRegistry`` class is the primary front end to the ``dataregistry`` package.
This should be the only object users have to import to their code. 

It connects the user to the database, and serves as a wrapper to both the
``Registrar`` and ``Query`` classes.

.. autoclass:: dataregistry.DataRegistry
   :members:

   .. automethod:: dataregistry.Registrar.get_owner_types
   .. automethod:: dataregistry.Query.find_datasets

.. automethod:: dataregistry.registrar.dataset.DatasetTable.register

.. automethod:: dataregistry.registrar.dataset.DatasetTable.replace

.. automethod:: dataregistry.registrar.dataset.DatasetTable.modify

.. automethod:: dataregistry.registrar.dataset.DatasetTable.delete

.. automethod:: dataregistry.registrar.dataset.DatasetTable.add_keywords

.. automethod:: dataregistry.registrar.dataset.DatasetTable.get_modifiable_columns

.. automethod:: dataregistry.registrar.dataset.DatasetTable.get_keywords

.. automethod:: dataregistry.registrar.execution.ExecutionTable.register

.. automethod:: dataregistry.registrar.dataset_alias.DatasetAliasTable.register
