.. _dregs_cli:

The ``dregs`` CLI
=================

The DESC data registry also comes with a Command Line Interface (CLI) tool,
``dregs``,  which can perform some simple actions.

See the :ref:`tutorials section <tutorials-cli>` for a demonstration of its usage.

Registering a new entry in the database
---------------------------------------

.. autoprogram:: cli.cli:arg_register
   :prog: dregs register

Listing datasets within the data registry
-----------------------------------------

The ``dregs ls`` command can be used to quickly list the datasets within the
DESC data registry. Two basic filters can be applied; on the `owner` and/or
`owner_type`. All entries can also be retured using the ``--all`` flag.

.. autoprogram:: cli.cli:arg_ls
   :prog: dregs ls
