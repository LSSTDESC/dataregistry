.. _installation:

Installation
============

Caveats
-------

- Currently the DESC data registry software can only be used at NERSC.
- One of the dependencies (psycopg2) cannot be installed easily with pip.

Therefore, if psycopg2 is not already in your environment, before doing the
pip install step below, use conda to install it, e.g.

.. code-block:: bash

   conda create -p ./dregs_env psycopg2


Main installation steps
-----------------------
To install the DESC data registry software first clone the GitHub repository

.. code-block:: bash

   git clone https://github.com/LSSTDESC/dataregistry.git

then, navigate to the ``dataregistry`` directory and install via *pip* using

.. code-block:: bash

   python3 -m pip install .

Authenticating with the database
--------------------------------

A one-time setup is required in order to authenticate with the DESC data
registry database. This is done via a YAML configuration file which stores the
connection information to the database, and a ``.pgpass`` file, which stores
user credentials.

First, make a file, (the *DREGS config file*), say
``~/.config_reg_access``, in your ``$HOME`` directory containing the entry

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
