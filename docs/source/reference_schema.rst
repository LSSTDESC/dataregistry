Database schema
===============

A description of the data registry schema layout. Note that each schema in the
database (e.g., the default and production schemas) follows the same structure.

.. image:: _static/schema_plot.png
   :alt: Image missing

The dataset table
-----------------

.. list-table::
   :header-rows: 1

   * - row
     - description
     - type
   * - ``dataset_id``
     - Unique identifier for dataset
     - int
   * - ``name``
     - User given name for dataset
     - str
   * - ``relative_path``
     - Relative path storing the data, relative to `<root_dir>` 
     - str
   * - ``version_major``
     - Major version in semantic string (i.e., X.x.x)
     - int
   * - ``version_minor``
     - Minor version in semantic string (i.e., x.X.x)
     - int
   * - ``version_patch``
     - Patch version in semantic string (i.e., x.x.X)
     - int
   * - ``version_suffix``
     - Optional version suffix
     - str
   * - ``dataset_creation_date``
     - Dataset creation date
     - datetime
   * - ``is_archived``
     - True if the data is archived, i.e, the data is longer within `<root_dir>`
     - bool
   * - ``is_external_link``
     - ???
     - bool
   * - ``is_overwritten``
     - True if the original data for this dataset has been overwritten at some point. This would have required that ``is_overwritable`` was set to ``true`` on the original dataset  
     - bool
   * - ``is_valid``
     - ???
     - bool
   * - ``register_date``
     - Date the dataset was registered 
     - datetime
   * - ``creator_uid``
     - `uid` (user id) of the person that registered the dataset
     - str
   * - ``access_API``
     - Describes the software that can read the dataset (e.g., "gcr-catalogs", "skyCatalogs")
     - str
   * - ``execution_id``
     - ID of execution this dataset belongs to
     - int
   * - ``description``
     - User provided description of the dataset
     - str
   * - ``owner_type``
     - Datasets owner type, can be "user", "group", "project" or "production".
     - str
   * - ``owner``
     - Owner of the dataset
     - str
   * - ``data_org``
     - Dataset organisation ("file" or "directory")
     - str
   * - ``nfiles``
     - How many files are in the dataset
     - int
   * - ``total_disk_space``
     - Total disk spaced used by the dataset
     - float

The dataset_alias table
-----------------------

.. list-table::
   :header-rows: 1

   * - row
     - description
     - type
   * - ``dataset_alias_id``
     - Unique identifier for alias
     - int
   * - ``name``
     - User given alias name
     - str
   * - ``dataset_id``
     - ID of dataset this is an alias for
     - int
   * - ``supersede_date``
     - ???
     - datetime
   * - ``register_date``
     - Date the dataset was registered
     - datetime
   * - ``creator_uid``
     - `uid` (user id) of the person that registered the dataset
     - str

The dependency table
--------------------

.. list-table::
   :header-rows: 1

   * - row
     - description
     - type
   * - ``dependency_id``
     - Unique identifier for dependency
     - int
   * - ``execution_id``
     - Execution this dependency is linked to
     - int
   * - ``input_id``
     - Dataset ID of the dependent dataset
     - int
   * - ``register_date``
     - Date the dependency was registered
     - datetime

The execution table
-------------------

.. list-table::
   :header-rows: 1

   * - row
     - description
     - type
   * - ``execution_id``
     - Unique identifier for execution
     - int
   * - ``description``
     - User given discription of execution
     - str
   * - ``name``
     - User given execution name
     - str
   * - ``register_date``
     - Date the execution was registered
     - datetime
   * - ``execution_start``
     - Date the execution started
     - datetime
   * - ``locale``
     - Locale of execution (e.g., NERSC)
     - str
   * - ``configuration``
     - Path to configuration file of execution
     - str
   * - ``creator_uid``
     - `uid` (user id) of the person that registered the dataset
     - str

The execution_alias table
-------------------------

.. list-table::
   :header-rows: 1

   * - row
     - description
     - type
   * - ``execution_alias_id``
     - Unique identifier for execution alias
     - int
   * - ``execution_id``
     - Execution this alias is linked to
     - int
   * - ``alias``
     - User given execution alias name
     - str
   * - ``register_date``
     - Date the execution was registered
     - datetime
   * - ``supersede_date``
     - ???
     - datetime
   * - ``creator_uid``
     - `uid` (user id) of the person that registered the dataset
     - str

The provenance table
--------------------

.. list-table::
   :header-rows: 1

   * - row
     - description
     - type
   * - ``provenance_id``
     - Unique identifier for provenance
     - int
   * - ``code_version_major``
     - Major version of code when this schema was created
     - int
   * - ``code_version_minor``
     - Minor version of code when this schema was created
     - int
   * - ``code_version_patch``
     - Patch version of code when this schema was created
     - int
   * - ``code_version_suffix``
     - Version suffix of code when this schema was created
     - str
   * - ``db_version_major``
     - Major version of database
     - int
   * - ``db_version_minor``
     - Minor version of database
     - int
   * - ``db_version_patch``
     - Patch version of database
     - int
   * - ``git_hash``
     - Git commit hash when this schema was created
     - str
   * - ``repo_is_clean``
     - Was repository clean when this schema was created
     - bool
   * - ``update_method``
     - "CREATE", "MODIFY" or "MIGRATE"
     - str
   * - ``schema_enabled_date``
     - When was the schema enabled
     - datetime
   * - ``creator_uid``
     - `uid` (user id) of the person that registered the schema
     - str
   * - ``comment``
     - Any comment
     - str
