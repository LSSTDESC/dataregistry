'''
Store major, minor, patch for db version expected by this code version as
well as associated comment for provenance table.
These quantities should be updated whenever there is a change to the schema
structure
The information is only needed when creating a new schema or when
modifying the schema in place
'''
_DB_VERSION_MAJOR = 3
_DB_VERSION_MINOR = 4
_DB_VERSION_PATCH = 0
_DB_VERSION_COMMENT = "Allow dataset.relative_path to be NULL"

__all__ = ["_DB_VERSION_MAJOR", "_DB_VERSION_MINOR", "_DB_VERSION_PATCH",
           "_DB_VERSION_COMMENT"]
