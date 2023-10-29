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

   .. automethod:: dataregistry.Registrar.register_dataset
   .. automethod:: dataregistry.Registrar.get_owner_types
   .. automethod:: dataregistry.Registrar.register_execution
   .. automethod:: dataregistry.Registrar.register_dataset_alias
   .. automethod:: dataregistry.Query.find_datasets

