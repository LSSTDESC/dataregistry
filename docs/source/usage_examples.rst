.. _usage:

Background
==========
Registered datasets are partitioned into *production* and *non-production* (aka
*working*).  These examples only concern non-production.  A non-production
dataset is uniquely identified by a **name** and a **version**
(both user-specified). And when it is registered it is also assigned a unique
**dataset_id**.  **name** is arbitrary except that it may not contain
certain non-alphanumberic characters like space, question mark, etc.
There is no default value.  **version** must be of the form
`X.Y.Z` where X, Y and Z are non-negative integers with the usual
semantic versioning meaning.  If not supplied it will default to "1.0.0".


Registering a dataset
=====================

The dataregistry supports several different modes of registering datasets.
The most common are described here.


Register with copy
------------------

When registering a "normal", non-production dataset, it is typically
copied from its original location to somewhere in cfs under the dataregistry
root, which at NERSC is `/global/cfs/cdirs/desc-reg`.  More specifically,
it will end up under
`/global/cfs/cdirs/desc-reg/lsst_desc_working/<owner-type>/<owner>`
where `<owner-type>` is one of "project", "group" or "user".  The value of
`<owner>` is up to the user, but for owner-type="user" it defaults to the
unix userid of the caller. This will be referred to as **entry_base_path**
below.

Note there are two constraints to be satisfied when registering new dataset:

- the pair (**name**, **version**) must not have been already used.

- the destination location for the new dataset must not have already been
  used.

(However, under certain circumstances you can *replace* a dataset, in which
case **name**, **version**, and destination location will all be the same
as before.)

Relative path specified
+++++++++++++++++++++++

The caller may specify the exact path relative to the **entry_base_path**
by means of the optional keyword argument `relative_path`:

.. code-block:: python

   import os
   from dataregistry import DataRegistry

   # create new data registry object; reference its registrar member
   my_reg = DataRegistry().registrar

   # establish values to be passed to the register routine
   name = "my_dataset"
   version = "1.0.1"

   # The dataset may be either a regular file or a directory.
   # This one is a regular file in my scratch area
   old_loc = os.path.join(os.getenv("SCRATCH"), "my_dataset.parquet")

   rel_path = "my_dataset.parquet"

   id_1, exec_1 = my_reg.dataset.register(
       name,
       version,
       description="A dataset registered with specific relative path",
       owner_type="user",  # this is the default; no need to specify
       old_location=old_loc,
       relative_path=rel_path,
       )

The result is a new entry in the dataset table in the database with
**dataset_id** == id_1, a new entry in the execution table with
**execution_id** == exec_1, and a copy of the dataset at
**entry_base_path**/my_dataset.parquet

To learn more about execution ids, see the
`pipelines tutorial <https://github.com/LSSTDESC/dataregistry/blob/main/docs/source/tutorial_notebooks/pipelines.ipynb>`__

Relative path unspecified
+++++++++++++++++++++++++

If the relative path is unspecified, the dataregistry will form one out of
name and version. Such generated relative paths always start with the
subdirectory `.gen_paths` followed by subdirectory `name`_`version`, then
the copy of the dataset at `old_location`:

The code for this would look just like the previous example except that
the `relative_path` argument would be omitted.  The resulting dataset
would have parent directory

**entry_base_path**/`name`_`version`/.gen_paths/`name`_`version`/my_dataset.parquet

The `.gen_paths` component guarantees that the generated paths will not
collide with user-supplied relative paths (which may not start with
`.gen_paths`).  The subdirectory whose name is manufactured out of
`name` and `version` guarantees generated paths will not conflict with
each other.


External datasets
-----------------

It is possible to register datasets whose files are not managed by the
dataregistry.  They need not even be located at NERSC. Reasons
for going this route include

- your dataset is dependent on a catalog whose official location is
  elsewhere, perhaps even maintained by a different collaboration.
- the dataset is large and is likely to be superseded by another version
  soon. It's not worth the time and effort to copy to cfs
- the dataset is registered with and intended to be accessed via the Butler
  but you would like to be able to find it using the dataregistry.

Naturally the dataregistry cannot archive such a dataset
nor protect it from being overwritten or deleted by someone else.

You can register a dataset as external by including the optional argument
`location_type` with value "external" (default is "dataregistry"). In
this case you are required to also include at least one of the optional
arguments `contact_email` or `url`, preferably both. `contact_email` should be
the email address of someone responsible for the dataset. `url` should be a
valid url which may be used to access the dataset, either directly, e.g.
downloading with curl, or by
referencing a web site which describes how to access and use the dataset.
(For datasets located at NERSC you can use a url starting with "file:///".)
For datasets of general interest be sure to put something sufficiently
informative for the `description` argument.

.. code-block:: python

   import os
   from dataregistry import DataRegistry

   # create new data registry object; reference its registrar member
   my_reg = DataRegistry().registrar

   # establish values to be passed to the register routine
   name = "external_dataset"
   version = "1.2.3"
   email = "JaneDoe@slac.stanford.edu"
   url = "file:///global/cfs/cdirs/lsst/groups/some_group/large_dataset"

   id_2, exec_2 = my_reg.dataset.register(
       name,
       version,
       description="Registered
       owner_type="user",  # this is the default
       location_type="external",
       contact_email=email,
       url=url,
       )

More registration options
-------------------------

Keywords
++++++++

You may define keywords or use existing keywords to label your datasets.
Keywords can be associated with datasets at the time you register them
(argument `keywords`) or any time thereafter.

Inputs
++++++

You can specify which inputs were used to create a dataset either at
the time you register the new dataset or at a later time.  See the
pipeline tutorial for details.

Access API
++++++++++

By design the dataregistry does not itself read datasets except to copy them;
it has no knowledge of their contents or structure.  However it is possible
to identify an *access API*, an application which can read the dataset.
Examples include GCRCatalogs and the Butler. Use the optional arguments
`access_api` and `access_api_configuration` when you register the dataset
to store the information needed. The value of `access_api` should be the
name of the facility which knows how to read the file.  The value of
`access_api_configuration` is the path to a text file containing whatever
further information is needed by the facility to read the dataset.
For GCRCatalogs it's a yaml file, the same one used normally by GCRCatalogs
to load a dataset; for other facilities any text file format will do as long
as it contains the information needed by the facility to make sense of the
dataset.
When you register the dataset, the dataregistry will read the text file
and store the contents. A user can then make a query to recover the
contents of the confiuration file and pass them to the facility.


Queries
=======

There are a few special query routines to return structural information,
e.g. `get_keyword_list` returns a list of all defined keywords for either
the production or non-production part of the database, but for the most
part one uses the general-purpose `find_datasets`. Use the `property_names`
argument to list all database columns you would like returned (defaults
to all columns in the `dataset` table).  Use the `filters` argument to
narrow down the rows for which values should be returned. A filter is
just a triple (`property_name`, `operator`, `value`).  See documentation
of the `Filter` class for details.

Simple query
------------

.. code-block:: python

   import os
   from dataregistry import DataRegistry

   # create new data registry object; reference its query member
   my_q = DataRegistry().query

   # When specifying columns, qualify with table name
   columns = ["dataset.dataset_id", "dataset.name", "dataset.relative_path",
              "dataset.access_api", "dataset.access_api_configuration"]

   # dataset.name must contain "dc2" (case insensitive)
   filters = [my_q.gen_filter("dataset.name", "~=", "*dc2*")]

   results = my_q.find_datasets(
                 property_names=columns,
                 filters=filters,
                 schema_mode="production",  # search only production
             )
   to_print = min(len(results["dataset.name"]), 5)
   for i in range(to_print):
        print(results["dataset.name"][i],
              results["dataset.relative_path"][i])

The result will be a pandas DataFrame with a column for each entry in
`columns`.

Query using keywords
--------------------

.. code-block:: python

   import os
   from dataregistry import DataRegistry

   # create new data registry object; reference its query member
   my_q = DataRegistry().query

   # When specifying columns, qualify with table name
   columns = ["dataset.dataset_id", "dataset.name", "dataset.relative_path",
              "version_string"]

   # dataset_id must be > 10; name must contain "dc2" (case insensitive)
   filters = [my_q.gen_filter("keyword.keyword", "==", "pz_model")]

   results = my_q.find_datasets(
                 property_names=columns,
                 filters=filters,
                 schema_mode="working",  # search only non-production
             )
   to_print = min(len(results["dataset.name"]), 5)

   for i in range(to_print):
       print(results["dataset.name"][i],
             results["dataset.version_string"][i],
             results["dataset.relative_path"][i])
