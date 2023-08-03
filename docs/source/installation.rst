.. _installation:

Installation
============

Currently the DESC data registry software can only be used at NERSC (i.e.,
PerlMutter).

Main installation steps
-----------------------

Some of the libraries needed (``libpq-dev`` for the ``psycopg2`` package) are
not natively available at NERSC, meaning the data registry cannot be easily
installed standalone with *pip*.

Thus we need to work within a *conda* environment. If you do not have a *conda*
environment already at NERSC, you can make a new one via 

.. code-block:: bash

   module load conda/Mambaforge-22.11.1-4
   conda create -p ./dregs_env psycopg2

where ``./dregs_env`` is the path where the environment will be installed
(change this as required). To activate the environment do

.. code-block:: bash

   conda activate <path to your env>

Now we can install the DESC data registry software. First clone the GitHub
repository

.. code-block:: bash

   git clone https://github.com/LSSTDESC/dataregistry.git

then, navigate to the ``dataregistry`` directory and install via *pip* using

.. code-block:: bash

   python3 -m pip install .

You can test to see if the ``dataregistry`` package installed successfully by
typing

.. code-block:: bash

   python3 -c "import dataregistry; print(dataregistry.__version__)"

and seeing the current package version printed to the console.

Authenticating with the database
--------------------------------

A one-time setup is required in order to authenticate with the DESC data
registry database. This is done via a YAML configuration file which stores the
connection information to the database, and a ``.pgpass`` file, which stores
user credentials.

First make a *DREGS config file*, we recommend a file named
``~/.config_reg_access`` stored in your ``$HOME`` directory, containing the
entry

.. code-block:: yaml

   sqlalchemy.url : postgresql://reg_writer@data-registry-dev-loadbalancer.jrb-test.development.svc.spin.nersc.org:5432/desc_data_registry

Then (if you don't have one already), create a file named ``~/.pgpass`` in your
``$HOME`` directory, and append the entry

.. code-block:: bash

   # data registry db
   data-registry-dev-loadbalancer.jrb-test.development.svc.spin.nersc.org:5432:desc_data_registry:reg_writer:<password>

where ``<password>`` is provided on demand by the DESC data registry admins. As
a final step, the ``.pgpass`` file must only be readable by you, which you
can ensure by doing

.. code-block:: bash

   chmod 600 .pgpass

.. note::

   If for some reason you need the *DREGS config file* to be in another
   location, you can let DREGS know about it by setting a ``DREGS_CONFIG``
   environment variable with the full path of the config file. If neither of
   these options are possible, there is always the option to manually specify
   where the configuration file is during runtime.
