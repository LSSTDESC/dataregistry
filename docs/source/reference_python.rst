The ``dataregistry`` package
============================

Reference documentation for the most commonly used functions in the
``dataregistry`` package.  Demonstrations of their usage can be found in the
examples section.

.. _registrar_class:

The Registrar class
-------------------

The ``dataregistry`` package comes with a ``Registrar`` class to allow users to
register datasets, executions, aliases and dependencies directly within your
Python code.

.. autofunction:: dataregistry.Registrar.register_dataset
   :noindex:

.. autofunction:: dataregistry.Registrar.register_execution
   :noindex:

.. autofunction:: dataregistry.Registrar.register_dataset_alias
   :noindex:

The Query class
---------------

The ``dataregistry`` package comes with a ``Query`` class to allow users to
query entries in the database.

.. autofunction:: dataregistry.Query.find_datasets
   :noindex:
