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

The typical user (DESC member) will authenticate to the DESC ``dataregistry``
database with the group account ``reg_writer``. This supports read and write
operations for development entries (schema ``lsst_desc_working``) and read
access to production entries (schema ``lsst_desc_production``).  It may also
be used when running tutorial notebooks. As long as you are a member of the
``lsst`` unix group at NERSC and are not planning to write production entries
(which takes extra privilege) you don't have to be concerned with any of this.
You will by default have the privileges you need.
