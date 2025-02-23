Welcome to the DESC data management's software documentation
============================================================

The data registry is a means of keeping track of DESC related datasets,
providing both a shared space at NERSC for the raw data, and a registry
database to store provenance information for that data, for example:

- where the data is located
- when the data was produced
- what precursor datasets it relies on

What and whom is it for?
------------------------

It is for any datasets for which provenance and accessibility are important, e.g.

- they are of general interest within the collaboration
- they are used as input to further analysis steps
- they are referenced in a paper

It is for anyone at DESC who needs to create, find or access such a dataset.

Getting started
---------------

This documentation is to help you get set up using the ``dataregistry`` Python
package; covering installation, how to register datasets, and how to query for
them. 

.. toctree::
   :maxdepth: 2
   :caption: Overview:
   :hidden:

   installation

.. toctree::
   :maxdepth: 2
   :caption: Tutorials:
   :hidden:

   tutorial_setup
   tutorial_python
   tutorial_cli
   cli_cheat_sheet

.. toctree::
   :maxdepth: 2
   :caption: Reference:
   :hidden:

   reference_python
   reference_cli
   reference_schema

.. toctree::
   :maxdepth: 2
   :caption: Developer notes:
   :hidden:

   dev_notes_spin
   dev_notes_database
   installation_locally

.. toctree::
   :maxdepth: 2
   :caption: Contact:
   :hidden:

   contact


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
