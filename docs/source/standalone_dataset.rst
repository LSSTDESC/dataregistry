Standalone datasets
===================

Below are some quick examples of how to enter standalone datasets into the DESC
data registry. By standalone we mean the data does not form part of a larger
pipeline (i.e., has no dependencies); for pipeline datasets that do have
dependencies see the next section.

In all the examples below we are assuming the default DREGS configuration (see
the :ref:`Installation page <installation>` for more details). 

Registering a dataset
---------------------

There are two ways to register a dataset within DREGS; using the
``dataregistry`` package directly from within Python, or via the DREGS CLI.

Using the ``dataregistry`` package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To register a dataset using the ``dataregistry`` package; first establish a
connection to the database using a ``DREGS`` class object (full details
:ref:`here <dregs_class>`), then register a dataset using the ``Registrar``
extension.

For example:

.. code-block:: python

   from dataregistry import DREGS
   
   # Establish connection to database (using defaults)
   dregs = DREGS()
   
   # Add new entry.
   new_id = dregs.Registrar.register_dataset(
       "my_desc_project/my_desc_dataset",
       "1.0.0",
       description="An output from some DESC code",
       old_location="/path/at/nersc/to/the/dataset/",
   )

Here we have copied the contents of the ``/path/at/nersc/to/the/dataset/``
directory and registered it under the relative path [#relpath]_
``my_desc_project/my_desc_dataset`` (version `1.0.0`) in the data registry.

We have given the dataset a custom description, but all other optional fields
for ``register_dataset`` have been left out, and therefore obtain the default
values. Perhaps most importantly of these are ``owner`` and ``owner_type`` [#ownertype]_,
which default to ``$USER`` and ``"user"`` respectively. A full list of
``Registrar.register_dataset`` options can be found :ref:`here <dregs_class>`.

The variable ``new_id`` stores the data registry ID for this dataset (useful if
you need to link it to an execution entry later).

If you are bulk registering a series of datasets at once you can set a
universal ``owner`` and ``owner_type`` for the lifetime of the instance, e.g.,

.. code-block:: python

   from dataregistry import DREGS

   # Establish connection to database (using set owner and owner_type)
   dregs = DREGS(owner="DESC CO group", owner_type="group")

All datasets registered during that instance will pick up the universal values
for ``owner`` and ``owner_type``, however the values passed to
``register_dataset`` directly will always take precedence.

.. note::
   If your DREGS configuration file is not located in the default location, and
   you do not have ``DREGS_CONFIG`` set, you will have to pass the location of
   your configuration file to the DREGS class on initialization, i.e., ``dregs
   = DREGS(config_file="/path/to/config")``.

.. [#relpath] The full path of the dataset in the data registry is constructed
   as follows
   ``<DREGS_ROOT>/<owner_type>/<owner>/<relative_path>``

.. [#ownertype] The allowed ``owner_type``'s are "user", "project", "group" or
   "production". 

Using the DREGS CLI
~~~~~~~~~~~~~~~~~~~

One can alternatively enter datasets using the DREGS CLI tool (see :ref:`here
<dregs_cli>` for more documentation on the CLI).  

For example, say I have produced some data from my latest DESC publication that
I want to archive/distribute via the data registry. My data is located at
``/some/place/at/nersc/my_paper_dataset/``, and I want to tag it as a
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

This will copy the entire ``/some/place/at/nersc/my_paper_dataset/`` directory
into the data registry with the path ``<DREGS_ROOT>/production/DESC Generic
Working Group/my_paper_dataset/``. As we did not specify a ``--name`` for the
dataset, the ``name`` column in the database will automatically be assigned as
``my_paper_dataset``. 

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
my_paper_dataset``, and tell the dataregistry to automatically bump the patch
version to ``1.0.1`` by specifying "patch" as the version string (you could
however have entered "1.0.1" here if you prefer).

.. note::

   Remember the relative paths in the data registry need to be unique, which is
   why we could not have the relative path of the second entry match the first.
   But for datasets only the ``name`` plus ``version`` has to be unique, which
   is how we could associate them with the same ``name`` column.

Querying the data registry
--------------------------

Currently, the only way to query the DESC data registry is via the
``dataregistry`` package.

As an example, say I want to query for the `my_paper_dataset` we entered above
using the CLI.

.. code-block:: python

   from dataregistry import DREGS
   
   # Establish connection to database (using defaults)
   dregs = DREGS()
   
   # Query 1: Query dataset name
   f = dregs.gen_filter('dataset.name', '==', 'my_paper_dataset')
   results = dregs.Query.find_datasets(['dataset.dataset_id', 'dataset.name', 'dataset.relative_path'], [f])

Which would return a SQL Alchemy results object containing our results. In our case this should be two entries, from the two versions of the dataset we entered above.

We could print the results to check using.

.. code-block:: python

   for r in results:
       print(r)

