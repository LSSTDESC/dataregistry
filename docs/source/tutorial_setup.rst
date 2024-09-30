Getting set up
==============

This section has some tutorials on how to get started using the
``dataregistry`` Python package, and the "``dregs``" Command Line Interface.

Before we begin, make sure that you have the ``dataregistry`` package installed
and available to you at NERSC (installation instructions :ref:`here
<installation>`). This is generally simplest working within the
``desc-python-bleed`` Conda environment.

Also, make sure you have completed the one-time-setup for the data registry
(more details :ref:`here <one-time-setup>`),
namely:

- You have added the database connection information in your
  ``~/.config_reg_access`` file
- You have added your database authentication information in your ``~/.pgpass``
  file

.. _interactive-notebooks:

Working with the data registry tutorial notebooks interactively
---------------------------------------------------------------

The tutorial notebooks for the ``dataregistry`` package work as stand alone
reference materials, however they can also be used interactively.

To do this at NERSC, first clone the ``dataregistry`` repository to your home
space, then load the notebooks through the NERSC JupyterHub portal. Note that
you will need to be operating within a kernel that has the ``dataregistry``
package installed, i.e., ``desc-python-bleed`` (see `here
<https://docs.nersc.gov/services/jupyter/how-to-guides/>`__ how to register
your own Python environments as Jupyter kernels).
