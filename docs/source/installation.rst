.. _installation:

Installation
============

Currently the DESC ``dataregistry`` database is only accessible through NERSC
(i.e., PerlMutter).

Using the ``dataregistry`` at NERSC
------------------------------------

The ``dataregistry`` package is readily available as part of the
``desc-python`` and ``desc-python-bleed`` environments (see `here
<https://confluence.slac.stanford.edu/display/LSSTDESC/Getting+Started+with+Anaconda+Python+at+NERSC>`__
for details about the *Conda* environments available at NERSC). Therefore
before getting started, make sure to activate one of these
environments from the command line, e.g.

.. code-block:: bash

   source /global/common/software/lsst/common/miniconda/setup_current_python.sh

or, when working at the NERSC JupyterHub, select the ``desc-python`` or
``desc-python-bleed`` kernel.

Normally ``desc-python`` is preferred unless you need a more recent
version of soome package in the environment.

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

Authenticating with the reg_reader account
------------------------------------------

If you don't expect to write any entries to the data (apart from when running
tutorials) you need only run a script once to set up authentication:

.. code-block:: bash

   $ /global/common/software/lsst/dbaccess/dataregistry/enable_reader.sh

This script will create a file ``~/.config_reg_access`` in your ``$HOME``
directory (unless that file already exists, in which case the script will
just complain and exit). Note that the file is protected: only you can
read it.  If you change the mode to allow others to read it the
``dataregistry`` software will not accept it.

Authenticating with the reg_writer account
------------------------------------------

A one-time setup is required in order to authenticate with the DESC data
registry database. This is done via a YAML configuration file which stores the
connection information to the database, and a ``.pgpass`` file, which
stores full user credentials, including password.  We expect this form
of the set-up to be used primarily by those authenticating with the
``reg_writer`` account, in which case ``reg_writer`` should be substituted
for ``<username>`` below, but it would work equally well for ``reg_reader``.

First, make a ``dataregistry`` configuration file. By default the data
registry code will look for a file named
``~/.config_reg_access`` stored in your ``$HOME`` directory, containing the
entry. (You can put your file somewhere else if you prefer but you will need
to specify its location when you make your connection using the Python API
or when you use the ``dregs`` CLI.)

.. code-block:: yaml

   sqlalchemy.url : postgresql://<username>@dataregistry-prod-loadbalancer.desc-dataregistry.production.svc.spin.nersc.org:5432/desc_data_registry 


Then, if you don't have one already, create a file named ``~/.pgpass`` in your
``$HOME`` directory, and append the entry

.. code-block:: bash

   # data registry db
   dataregistry-prod-loadbalancer.desc-dataregistry.production.svc.spin.nersc.org:5432:desc_data_registry:<username>:<password>

where ``<password>`` appropriate to the ``<username>`` will be provided on
request to the DESC data registry admins. As
a final step, the ``.pgpass`` file must only be readable by you, which you can
ensure by doing

.. code-block:: bash

   chmod 600 ~/.pgpass

