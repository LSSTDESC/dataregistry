.. _dregs_cli:

The `DREGS` CLI
===============

Registering a dataset
---------------------

Users can register datasets to the DESC data registry via the terminal using
the ``dataregistry/scripts/register_cli.py`` script.

Usage is as follows: 

.. code-block:: bash

   python3 register_cli.py <relpath> <version> --optional_args

.. option:: <relpath>

   Destination for the dataset within the data registry. Path is relative to
   ``<registry root>/<owner_type>/<owner>``.

   :type: string
   :required: True

.. option:: <version>

   Semantic version string of the format MAJOR.MINOR.PATCH *or*
   a special flag "patch", "minor" or "major".

   When a special flag is used it automatically bumps the relative
   version for you (see examples for more details).

   :type: string
   :required: True

.. option:: --version_suffix <version_suffix>

   Optional suffix string to place at the end of the version string.
   Cannot be used for production datasets.

   :type: string

.. option:: --owner-type <owner_type>

   Type of owner for this dataset. Can be "user", "group" or "production".

   :type: string
   :default: "user"

.. option:: --owner <owner>

   Who is the owner of this dataset?

   :type: string
   :default: $USER

.. option:: --locale <locale>

   Where was this dataset produced?

   :type: string
   :default: "NERSC"

.. option:: --is-overwritable

   Flag indicating the dataset can be overwritten in the future. Must be False
   for production.

   :type: bool
   :default: True for "user" and "group". False for "production".

.. option:: --creation_date <creation_date>

   Manually set creation date of dataset.

   :type: datetime
   :default: Current datetime

.. option:: --description <description>

   Description of dataset.

   :type: string

.. option:: --old_location <path>

   Path to dataset being entered into the data registry

   :type: string

.. option:: --make-sym-link

   Flag to make a symlink to the dataset rather than copy the data to the
   registry

.. option:: --schema-version <schema>

   Schema to use (for testing purposes only)

   :type: string

.. option:: --is_dummy

   Flag a dataset as a dummy entry (for testing purposes only)
