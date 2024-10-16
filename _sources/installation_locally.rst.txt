Local installation
==================

.. _local-installation:

When installing the ``dataregistry`` package locally yourself, it is
recommended to work within your own Conda or Python virtual environment.

Creating a Conda environment 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can make a new Conda environment via 

.. code-block:: bash

   module load conda/Mambaforge-22.11.1-4
   conda create -p ./datareg_env psycopg2

where ``./datareg_env`` is the path where the environment will be installed
(change this as required). To activate the environment do

.. code-block:: bash

   conda activate <path to your env>

Creating a Python venv
~~~~~~~~~~~~~~~~~~~~~~

or, you can work within a Python virtual environment via

.. code-block:: bash

   module load python/3.10
   python3 -m venv ./datareg_env

where ``./datareg_env`` is the path where the environment will be installed
(change this as required). To activate the environment do

.. code-block:: bash

   source <path to your env>/bin/activate

Note the specific version of Python used above (``3.10``) is only an example,
the ``dataregistry`` package is supported on Python versions ``>=3.9``.

Installing the ``dataregistry`` package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to install the ``dataregistry`` package is via `PyPi
<https://pypi.org/project/lsstdesc-dataregistry/>`__, i.e.,

.. code-block:: bash
   
   pip install lsstdesc-dataregistry

Install from source
~~~~~~~~~~~~~~~~~~~

To install from source, first clone the GitHub repository

.. code-block:: bash

   git clone https://github.com/LSSTDESC/dataregistry.git

then, navigate to the ``dataregistry`` directory and install via *pip* using

.. code-block:: bash

   python3 -m pip install .

You can test to see if the ``dataregistry`` package has installed successfully
by typing

.. code-block:: bash

   python3 -c "import dataregistry; print(dataregistry.__version__)"

If you see the current package version printed to the console, success!
