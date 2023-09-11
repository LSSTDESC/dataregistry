The DREGS CLI
=============

The DREGS Command Line Interface (CLI) is a convenient way to perform some
basic tasks with the data registry directly from the command line.

First, make sure the ``dataregistry`` package is installed (see :ref:`here
<installation>` for details), after which the ``dregs`` command will be available.
To check, type

.. code-block:: bash

   dregs --help

to display the list of available commands for ``dregs``.

The full reference documentation for the DREGS CLI command options can be found
:ref:`here <dregs_cli>`, below we cover some example use cases.

Registering a dataset
---------------------

Registering a dataset using the DREGS CLI is essentially identical to using the
``dataregistry`` package from within Python (it is simply calling the
``Registrar.register_dataset()`` function with arguments from the command
line).  Therefore reviewing the `first tutorial notebook
<https://github.com/LSSTDESC/dataregistry/blob/main/docs/source/tutorial_notebooks/DREGS_tutorial_NERSC.ipynb>`_
is the best place to get familiar with the registration process and for
descriptions of the associated dataset metadata.

Typing

.. code-block:: bash

   dregs register dataset --help

will list all the metadata properties that can be associated with a dataset
being registered. Similar to registering datasets using the ``dataregistry``
package, the ``relative_path`` and ``version`` string properties are mandatory,
which will always be the first two parameters passed to the ``dregs register
dataset`` command respectively.  

For example, say I have produced some data from my latest DESC publication that
I want to archive/distribute via the data registry. My data are located in the
directory ``/some/place/at/nersc/my_paper_dataset/``, and I want to tag it as a
production dataset owned by the `DESC Generic Working Group`. To do this I
would run the DREGS CLI as follows:

.. code-block:: bash

   dregs register dataset \
      my_paper_dataset \
      1.0.0 \
      --old-location /some/place/at/nersc/my_paper_dataset/ \
      --owner_type production \
      --owner "DESC Generic Working Group" \
      --description "Data from my_paper_dataset" 

This will recursively copy the ``/some/place/at/nersc/my_paper_dataset/``
directory into the data registry under the relative path ``my_paper_dataset``.
As we did not specify a ``--name`` for the dataset, the ``name`` column in the
database will automatically be assigned as ``my_paper_dataset`` (and all other
properties we did not specify will keep their default values). 

Updating a dataset
------------------

Now say a few months later a bug has been discovered in the
``my_paper_dataset`` data and the entry needs to be updated. As we entered
``my_paper_dataset`` as a production dataset we cannot directly overwrite the
data, however we can create a new version of the dataset as follows.

.. code-block:: bash

   dregs register dataset \
      my_paper_dataset_updated \
      patch \
      --old-location /some/place/at/nersc/my_paper_dataset_updated/ \
      --owner_type production \
      --owner "DESC Generic Working Group" \
      --description "Data from my_paper_dataset describing bugfix" \
      --name my_paper_dataset

Here we associate it with the previous dataset through ``--name
my_paper_dataset``, and tell the data registry to automatically bump the patch
version to ``1.0.1`` by specifying "patch" as the version string (you could
however have entered "1.0.1" here if you prefer).

.. note::

   Remember the relative paths in the data registry need to be unique, which is
   why we could not have the relative path of the second entry match the first.
   But for datasets only the ``name`` plus ``version`` has to be unique, which
   is how we could associate them with the same ``name`` column.

Querying the data registry
--------------------------

We can also do some simple querying via the DREGS CLI to see what datasets we,
or others, have in the data registry.

We can do this using the ``dregs ls`` command (type ``dregs ls --help`` for more
info).

By default, typing

.. code-block:: bash

   dregs ls

will list all the datasets registered in DREGS by "you" (i.e.,
where ``owner == $USER``). To be more precise, you can specify the ``owner``
and/or ``owner_type`` you want to list the datasets for. 

For example, to see all the datasets from the DESC Generic Working Group we would do

.. code-block:: bash

   dregs ls --owner "DESC Generic Working Group"

To list entries from all owners type

.. code-block:: bash

   dregs ls --all

Using ``dregs ls`` is a quick an easy way to remind yourself what names you
gave to previous datasets, and what relative paths they reside at.
