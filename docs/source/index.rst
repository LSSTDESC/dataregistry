Welcome to the DESC data management's software documentation
============================================================

What is it?
-----------

The data registry is a means of keeping track of DESC related datasets,
providing both a shared space at NERSC for the raw data, and a registry database
to store provenance information for that data, for example:

- where the data is located
- when the data was produced
- what precursor datasets is relies on

Who is it for?
--------------

It is for anyone who creates a dataset for which provenance and accessibility
are important, e.g.

- it is of general interest within the collaboration
- it is used as input to further analysis steps
- it is referenced in a paper

...and anyone who needs to find and access such a dataset.

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

.. toctree::
   :maxdepth: 2
   :caption: Reference:
   :hidden:

   reference_python
   reference_cli
   reference_schema

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
