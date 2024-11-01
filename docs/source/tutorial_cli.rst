.. _tutorials-cli:

The ``dataregistry`` CLI
========================

The ``dataregistry`` Command Line Interface (CLI) is a convenient way to
perform some basic tasks with the data registry directly from the command line.

First, make sure the ``dataregistry`` package is installed (see :ref:`here
<installation>` for details), after which the ``dregs`` command will be available.
To check, type

.. code-block:: bash

   dregs --help

to display the list of available commands for ``dregs``.

The full reference documentation for the ``dataregistry`` CLI command options
can be found :ref:`here <dregs_cli>`, below we cover some example use cases.

Registering a dataset
---------------------

Registering a dataset using the CLI is essentially identical to using the
``dataregistry`` package from within Python (it is simply calling the
``Registrar.register_dataset()`` function with arguments from the command
line).  Therefore reviewing the `first tutorial notebook
<https://github.com/LSSTDESC/dataregistry/blob/main/docs/source/tutorial_notebooks/getting_started.ipynb>`_
is the best place to get familiar with the registration process and for
descriptions of the associated dataset metadata.

Typing

.. code-block:: bash

   dregs register dataset --help

will list all the metadata properties that can be associated with a dataset
during registration. As when registering datasets using the ``dataregistry``
package, the dataset ``name`` and ``version`` properties are mandatory, which
will always be the first two parameters passed to the ``dregs register
dataset`` command respectively.  

For example, say I have produced some data from my latest DESC publication that
I want to archive/distribute via the data registry. My data are located in the
directory ``/some/place/at/nersc/my_paper_dataset/``, and I want to tag it as a
project dataset owned by the `DESC Generic Working Group`. To do this I
would run the CLI as follows:

.. code-block:: bash

   dregs register dataset \
      my_paper_dataset \
      1.0.0 \
      --old-location /some/place/at/nersc/my_paper_dataset/ \
      --owner_type project \
      --owner "DESC Generic Working Group" \
      --description "Data from my_paper_dataset" 

This will recursively copy the ``/some/place/at/nersc/my_paper_dataset/``
directory into the data registry shared space with the
``name='my_paper_dataset'`` (other non-specified properties will keep their
default values). 

Updating a dataset
------------------

Now say a few months later a bug has been discovered in the
``my_paper_dataset`` data and the entry needs to be updated. As we did not
deliberately specify that ``my_paper_dataset`` could be overwritten during the
initial registration, we need to create a new version of the dataset.

.. code-block:: bash

   dregs register dataset \
      my_paper_dataset \
      patch \
      --old-location /some/place/at/nersc/my_paper_dataset_updated/ \
      --owner_type project \
      --owner "DESC Generic Working Group" \
      --description "Data from my_paper_dataset describing bugfix" \

Here we associate it with the previous dataset through ``name=
my_paper_dataset`` (and making sure we keep the same `owner` and `owner_type`),
and tell the data registry to automatically bump the patch version to ``1.0.1``
by specifying "patch" as the version string (you could however have entered
"1.0.1" here if you prefer).

Querying the data registry
--------------------------

We can also do some simple querying via the CLI to see what datasets we, or
others, have in the data registry.

We can do this using the ``dregs ls`` command (type ``dregs ls --help`` for more
info).

By default, typing

.. code-block:: bash

   dregs ls

will list all the datasets registered by "you" (i.e., where ``owner ==
$USER``). To be more precise, you can specify the ``owner`` and/or
``owner_type`` you want to list the datasets for. 

For example, to see all the datasets from the DESC Generic Working Group we would do

.. code-block:: bash

   dregs ls --owner "DESC Generic Working Group"

To list entries from all owners do ``--owner none``.

You can search against the ``dataset.name`` column, with wildcard support,
where ``*`` is the wildcard character, e.g.,

.. code-block:: bash

   dregs ls --name dataset:dc2:*

will search for all datasets whose name starts with the pattern "dataset:dc2:"
(note the ``--name`` queries here are case insensitive).

To select what columns are printed in the result use the ``--return_cols`` option, e.g.,

.. code-block:: bash

   dregs ls --return_cols dataset_id name description status

Using ``dregs ls`` is a quick an easy way to remind yourself what names you
gave to previous datasets, and what relative paths they reside at.
