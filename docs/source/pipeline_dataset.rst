Pipeline datasets
=================

Let's go through an example of how to register data into `DREGS` from a
complete end-to-end pipeline. A "pipeline" in this context is any collection of
datasets that are inter-dependent, i.e., the output data from one process feeds
into the next process as its starting point. For example, a pipeline could
start with some raw imagery from a telescope, this raw imagery is then reduced
and fed into a piece of software that outputs a human-friendly value added
catalog. Or, a pipeline could be from a numerical simulation, starting with the
simulation's initial conditions, which then feed into an N-body code, which
then feed into a structure finder and gets reduced to a halo catalog.   

In the DESC data registry nomenclature, each stage of a pipeline is an
"execution", the data product(s) produced during each execution are "datasets",
and executions are linked to one another via "dependencies". 

For this example, we have a pipeline comprising of three stages. In the first
stage three datasets are produced, one dataset is a directory structure, and
the remaining two are individual files. The data output from the first stage
feeds into the second stage as input, which in turn produces its own output (in
this case a directory structure). Finally, the output data from stage two is
fed into the third stage as input and produces its own output dataset directory
structure. Thus our three stages have a simple sequential linking structure;
`Stage1 -> Stage2` and `Stage2 -> Stage3`.

How then would we go about inputting this pipeline into the DESC data registry?
Below is a graphical representation of the entries in `DREGS` we will need to
create, which we go through step-by-step below.

.. image:: _static/pipeline_example.png
   :alt: Image missing

There are two ways to insert entries into the DESC data registry; via the
`DREGS` Python package, or via the command line. 

For the purpose of this example we will be storing all the data of our pipeline
under our own user space (in `DREGS` nomenclature `owner_type = "user"`) under
the relative path `./my-desc-pipeline`.

Option 1: Using the ``dataregistry`` Python package
---------------------------------------------------

The first option is to create the database entries using the ``dataregistry``
Python package. 

To begin we need to get set up; importing the correct modules, referencing our
`DREGS` configuration file., connecting to the `DREGS` database, and creating a
``Registrar`` object.

.. code-block:: python

   import os
   import sys
   from dataregistry.registrar import Registrar
   from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

   DREGS_CONFIG = os.getenv("DREGS_CONFIG")

   # Establish connection to database
   engine, dialect = create_db_engine(config_file=DREGS_CONFIG)

   # Create Registrar object
   registrar = Registrar(
       engine, dialect, _lookup[owner_type], owner=owner, schema_version=SCHEMA_VERSION
   )

Now we can enter our database entries, starting with an `execution` entry to
represent the first stage of our pipeline.

.. code-block:: python

   name = "pipeline-stage-1"
   description = "The first stage of my pipeline"

   ex1_id = registrar.register_execution(
       name,
       description=description,
   ) 

where ``ex1_id`` is the `DREGS` index for this execution which we will reference later.

Next, we register the datasets associated with the output of ``pipeline-stage-1``:

.. code-block:: python

   name = "Dataset 1.1"
   description = "A directory structure output from pipeline stage 1"
   relpath = "my-first-pipeline/dataset_1p1/"
   version = "0.0.1"
   old_location = "/somewhere/on/machine/my-dataset/"

   dataset_id1 = registrar.register_dataset(
       relpath,
       version,
       description=description,
       old_location=old_location,
       execution_id=ex1_id,
       name=name,
   )

   name = "Dataset 1.2"
   description = "A file output from pipeline stage 1"
   relpath = "my-first-pipeline/dataset_1p2.db"
   version = "0.0.1"
   old_location = "/somewhere/on/machine/other-datasets/database.db"

   dataset_id2 = registrar.register_dataset(
       relpath,
       version,
       description=description,
       old_location=old_location,
       execution_id=ex1_id,
       name=name,
   )

   name = "Dataset 1.3"
   description = "Another file output from pipeline stage 1"
   relpath = "my-first-pipeline/dataset_1p3.hdf5"
   version = "0.0.1"
   old_location = "/somewhere/on/machine/other-datasets/info.hdf5"

   dataset_id3 = registrar.register_dataset(
       relpath,
       version,
       description=description,
       old_location=old_location,
       execution_id=ex1_id,
       name=name,
   )

Now, the `execution` for stage two of our pipeline. Note this will
automatically generate a dependency between the two executions.

.. code-block:: python

   name = "pipeline-stage-2"
   description = "The second stage of my pipeline"

   ex2_id = registrar.register_execution(
       name,
       description=description,
       input_datasets=[dataset_id1,dataset_id2,dataset_id3],
   )

and then to finish, we repeat the process for the remaining datasets

.. code-block:: python

    name = "Dataset 2.1"
    description = "A directory structure output from pipeline stage 2"
    relpath = "my-first-pipeline/dataset_2p1"
    version = "0.0.1"
    old_location = "/somewhere/on/machine/my-second-dataset/"
 
    dataset_id4 = registrar.register_dataset(
        relpath,
        version,
        description=description,
        old_location=old_location,
        execution_id=ex2_id,
        name=name,
    )

    name = "pipeline-stage-3"
    description = "The third stage of my pipeline"
 
    ex3_id = registrar.register_execution(
        name,
        description=description,
        input_datasets=[dataset_id4],
    )
 
    name = "Dataset 3.1"
    description = "A directory structure output from pipeline stage 3"
    relpath = "my-first-pipeline/dataset_3p1"
    version = "0.0.1"
    old_location = "/somewhere/on/machine/my-third-dataset/"
 
    dataset_id5 = registrar.register_dataset(
        relpath,
        version,
        description=description,
        old_location=old_location,
        execution_id=ex3_id,
        name=name,
    )

This process can be a bit cumbersome for entering data manually. However the
Python interface allows users to directly register data into `DREGS` within
their pipeline software. For those entering datasets into `DREGS` manually, the
CLI is likely a better option. 

Option 2: Using the `DREGS` CLI
-------------------------------

TBA
