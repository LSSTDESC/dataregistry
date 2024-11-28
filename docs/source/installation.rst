.. _installation:

Installation
============

Currently the DESC ``dataregistry`` database is only accessible through NERSC
(i.e., PerlMutter).

Using the ``dataregistry`` at NERSC
------------------------------------

The ``dataregistry`` package is readily available as part of the
``desc-python-bleed`` environment (see `here
<https://confluence.slac.stanford.edu/display/LSSTDESC/Getting+Started+with+Anaconda+Python+at+NERSC>`__
for details about the *Conda* environments available at NERSC). Therefore
before getting started, make sure to activate the ``desc-python-bleed``
environment from the command line, or, when working at the NERSC JupyterHub,
select the ``desc-python-bleed`` kernel. 

If you wish to install the ``dataregistry`` package yourself, see the
instructions :ref:`here <local-installation>`. 

Access accounts
---------------

Authentication to the DESC ``dataregistry`` database works through two primary
group accounts, ``reg_reader`` and ``reg_writer``. These accounts have
different privileges depending on the database schema you are connected to.
Both ``reg_reader`` and ``reg_writer`` have query (read) access to the primary
working schema (``lsst_desc_working``), but only ``reg_writer`` has write
access to register new entries in the database. 

Both ``reg_reader`` and ``reg_writer`` accounts have read and write access to
the tutorial schemas (used for the tutorial notebooks, i.e., the
``tutorial_working`` and ``tutorial_production`` schemas).

Neither ``reg_reader`` or ``reg_writer`` can write to the main production
schema (``lsst_desc_production``), however they both have read access. If you
need to register production entries, please consult one of the data registry
admins. 

Depending on which account you have access to, you will need to perform a
one-time-setup to authenticate, detailed below.

.. _one-time-setup:

Authenticating with the database
--------------------------------

A one-time setup is required in order to authenticate with the DESC data
registry database. This is done via a YAML configuration file which stores the
connection information to the database, and a ``.pgpass`` file, which stores
user credentials.

First, make a ``dataregistry`` configuration file. We recommend a file named
``~/.config_reg_access`` stored in your ``$HOME`` directory, containing the
entry

.. code-block:: yaml

   sqlalchemy.url : postgresql://<username>@dataregistry-release-test-loadbalancer.mcalpine-test.development.svc.spin.nersc.org:5432/desc_data_registry 

where ``<username>`` should either be ``reg_writer`` or ``reg_reader``,
depending on what account you have access to.

Then (if you don't have one already), create a file named ``~/.pgpass`` in your
``$HOME`` directory, and append the entry

.. code-block:: bash

   # data registry db
   dataregistry-release-test-loadbalancer.mcalpine-test.development.svc.spin.nersc.org:5432:desc_data_registry:<username>:<password>

where ``<password>`` is provided on demand by the DESC data registry admins. As
a final step, the ``.pgpass`` file must only be readable by you, which you can
ensure by doing

.. code-block:: bash

   chmod 600 ~/.pgpass
