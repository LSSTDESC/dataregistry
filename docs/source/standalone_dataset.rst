Standalone datasets
===================

Here are some quick example's of how to enter standalone datasets into the DESC
data registry. By standalone we mean the data does not form part of a larger
pipeline (i.e., has no dependencies), for pipeline datasets see the next
section.

Registering a dataset
---------------------

Using the ``dataregistry`` package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can enter datasets into the ``dataregistry`` directly via Python. This
could be part of a standalone script to enter data (though the CLI might be
more practical in this case, see below), or as an extension of your code.

To register a dataset use the ``Registrar`` class from the ``dataregistry``
package (full documentation here). For example:

.. code-block:: python

   from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine, ownertypeenum
   from dataregistry.registrar import Registrar

   # Establish connection to database
   engine, dialect = create_db_engine(config_file=DREGS_CONFIG)

   registrar = Registrar(
       engine, dialect, ownertypeenum.user, owner="user", schema_version=SCHEMA_VERSION
   )

   # Add new entry.
   new_id = registrar.register_dataset(
       relpath,
       version,
       version_suffix=version_suffix,
       name=name,
       creation_date=creation_data,
       description=description,
       old_location=old_location,
       copy=(not make_sym_link),
       is_dummy=is_dummy,
       execution_id=execution_id,
       verbose=True,
   )

Using the `DREGS` CLI
~~~~~~~~~~~~~~~~~~~~~

One can alternatively enter datasets using the `DREGS` CLI tool (see :ref:`here
<dregs_cli>` for full documentation on the tool).  

For example, say I have produced some data from my latest DESC publication that
I want to archive/distribute via the data registry. My data is located at
``/some/place/at/nersc/my_paper_dataset/``, and I want to tag it as a
production dataset owned by the `DESC Generic Working Group`. To do this I
would run the `DREGS` CLI as follows:

.. code-block:: bash

   dregs register \
      my_paper_dataset \
      1.0.0 \
      --old-location /some/place/at/nersc/my_paper_dataset/ \
      --owner_type production \
      --owner "DESC Generic Working Group" \
      --description "Data from my_paper_dataset" 

This will copy the entire ``/some/place/at/nersc/my_paper_dataset/`` directory
into the data registry with the relative path ``<registry root>/production/DESC
Generic Working Group/my_paper_dataset/``. As we did not specify a ``--name``
for the dataset, the ``name`` column in the database will automatically be
assigned as ``my_paper_dataset``. 

Now say a few months later a bug has been discovered in the
``my_paper_dataset`` data and the entry needs to be updated. As we entered
``my_paper_dataset`` as a production dataset we cannot directly overwrite the
data, however we can create a new version of the dataset as follows.

.. code-block:: bash

   dregs register \
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

.. code-block:: python

   from dataregistry.query import Query, Filter
   from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

   # Establish connection to database
   engine, dialect = create_db_engine(config_file=DREGS_CONFIG)

   # Create query object
   q = Query(engine, dialect, schema_version=SCHEMA_VERSION)

   # Query 1: Query dataset name
   f = Filter('dataset.name', '==', 'DESC dataset 1')
   results = q.find_datasets(['dataset.dataset_id', 'dataset.name'], [f])


