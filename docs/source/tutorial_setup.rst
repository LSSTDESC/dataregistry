Getting set up
==============

This section has some tutorials on how to get started using the
``dataregistry`` Python package, and the DREGS CLI.

Before we begin, make sure that you have the ``dataregistry`` package installed
and available to you at NERSC (installation instructions `here
<http://lsstdesc.org/dataregistry/installation.html>`__). This is generally
simplest within a *Conda* or Python virtual environment (note that the
``dataregistry`` is not yet part of the `desc-python` environment).

Also, make sure you have completed the one-time-setup for DREGS (more details
`here <http://lsstdesc.org/dataregistry/installation.html>`__), namely:

- You have added the database connection information in your
  ``~/.config_reg_access`` file
- You have added your database authentication information in your ``~/.pgpass``
  file

Working with notebooks interactively
------------------------------------

The tutorial notebooks for the ``dataregistry`` package work as stand alone
reference materials, however they can also be used interactively.

To do this at NERSC, first clone the ``dataregistry`` repository to your home
space, then load the notebooks through the NERSC JupyterHub. Note that you will
need to be operating within a kernel that has the ``dataregistry`` package
installed (see `here
<https://docs.nersc.gov/services/jupyter/how-to-guides/>`__ how to register
your own Python environments as Jupyter kernels).
