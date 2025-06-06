## Version 1.2.2
- Bug fix affecting querying of aliases
- Add test to check that alias querying works even when query_mode is not "both"

## Version 1.2.1
- Add Python 3.13 to CI and change Python version restriction to just a lower
  bound (>=3.9).
- Added logging. There is now a `logger` object in the `DbConnection` class
  that can be called for logging. For example
  `DbConnection.logger.debug("text")`.  This replaces the `verbose` flag.
- Users can now make aggregate queries via e.g.,
  `datareg.Query.aggregate_datasets("dataset_id", agg_func="avg")`. Aggregate
  function can be "count", "avg", "sum", "min" or "max". The usual query
  filters can still be applied. By default it is the `dataset` table the is
  queried, however setting `table_name=` can change this to "dataset_alias",
  "keyword", or "dataset_keyword".
- Expanded the list of columns that are modifiable.
- There is now a `get_all_tables()` and `get_all_columns()` function in the
  `Query` object. These can be reffined to return on the columns of a given
  table (default is `dataset`), or columns from all tables.

## Version 1.2.0

- Make `dataset.relative_path` nullable
- Adjust code to set `dataset.relative_path` to null for datasets of
  location type `exteranl` and `meta_only`
- Add documentation for in-place database upgrades
- Add script for making db upgrade

## Version 1.1.1

- Move `get_keyword_list()` function to the `Query()` class.
- Fix `dregs ls` for corner case scenarios
- Fix `dregs show` to work with `query_mode` format
- Added `skip_provenance_reflect` to the `DbConnection()` object, which stops
  the provenance table being queried during database connection. This is
  desgined for schema creation only.

## Version 1.1.0

When connection to the database, now a namespace is connected to, rather than a
selected schema. The namespace is a combination of a working and production
schema. On connection, both schemas are connected to and both schemas are
reflected. By default, queries search both the working and production schemas
and their results combined (this behaviour can be changed using `query_mode` to
limit queries to a single schema).

`register_mode` dictates which schema will be used ("working" or "production")
for write, modify and delete operations on entries during the connection
instance.

## Version 1.0.5

Update delete functionality

- The `delete()` function now takes `name`, `version_string`, `owner` and
  `owner_type` as arguments, rather than simply the `dataset_id`.
- One can still delete by the `dataset_id` using the CLI, which now includes a
  confirmation step.

## Version 1.0.4

Make more options for querying

- There is now a `~=` query operator that can utalise the `.ilike` filter to
  allow non-case-sensitive filering with wildcards (i.e., the `%` character).
- `dregs ls` can now filter on the dataset name, including `%` wildcards, using
  the `--name` option.
- `dregs_ls` can return arbitrary columns using the `--return_cols` option

## Version 1.0.3

Some changes to the way the `relative_path` is automatically generated from the
`name` and `version`.

- All automatically generated `relative_paths` are placed in a top level
  `.gen_paths` directory, e.g.,
  `<root_dir>/<schema>/<owner_type>/<owner>/.gen_paths`. This is to prevent
  clashes with user specified `relative_paths`.
- Single files no longer have their filename changed when automatically
  generating the `relative_path`. Instead a directory containing the `name` and
  `version` is created, and the file is copied there. This preserves the
  filname suffix in the relative path.

## Version 1.0.2

Update documentation for release, new installation instructions etc.

## Version 1.0.0 (Release)

- Update default NERSC site to
  `/global/cfs/cdirs/lsst/utilities/desc-data-registry`
- Update default schema names (now stored in
  `src/dataregistry/schema/default_schema_names.yaml`
- There is now a `reg_admin` account which is the only account to create the
  initial schemas. The schema creation script has been updated to give the
  correct `reg_writer` and `reg_reader` privileges.
- Remove `version_suffix`

## Version 0.6.4

- Update `dregs ls` to be a bit cleaner. Also has `dregs ls --extended` option
  to give back more quantities. Also can now query on a keyword using `dregs ls
  --keyword <keyword>`
- Added `modify` to CLI to update datasets from the command line

## Version 0.6.3

There cannot be a unique constraint in the database for the `owner`,
`owner_type` and `relative_path`, as multiple entries can share theose values,
however we require that at any one time only one dataset has their data at this
location. Added a check during register to ensure the `relative_path` is
avaliable.

## Version 0.6.2

- Bump database version to 3.3.0, removed `is_overwritten`, `replace_date`,
  `replace_uid` columns
- Added `replaced` bit to the `valid` bitmask

## Version 0.6.1

The `tables_required` list, when doing a query, was only build from the return
column list. This means if a filter used a table not in the returned column
list the proper join would not be made. This has been corrected.

## Version 0.6.0

- Added `replace()` function for datasets. This is functionally very similar to
  `register()`, but it allows users to overwrite previous datasets whilst
  keeping the same name/version/suffix/owner/ownertype combination.
  Documentation updated.
- Datasets now have a `replace_iteration` counter and a `replace_id` value
  which points to the dataset that replaced them. To reflect that the unique
  constraints now include the `replace_iteration` column.
- Database version bumped to 3.2.0
- Tests now use the `property_dict` return type and first make sure that the
  correct number of results was found before checking the results.

## Version 0.5.3

- Update the `schema.yaml` file to include unique constraints and table
  indexes.
- Update the unique constraints for the dataset table to be `owner`,
  `owner_type`, `name`, `version`, `version_suffix`.

## Version 0.5.2

When registering a dataset that is overwriting a previous dataset, don't tag
the previous datasets as `valid=False` until any data copying is successful.

## Version 0.5.1

Add ability to tag datasets with keywords/labels to make them easier to
catagorize.

- Can tag keywords when registering datasets through the Python API or CLI. Can
  add keywords after registration using the `add_keywords()` method in the
  Python API.
- Database version bumped to 3.0.0
- New table `keyword` that stores both the system and user keywords.
- New table `dataset_keyword` that links keywords to datasets.
- System keywords are stored in `src/dataregistry/schema/keywords.yaml`, which
  is used to populate the `keywords` table during database creation.
- Added `datareg.Registrar.dataset.get_keywords()` function to return the list
  of currently registered keywords.
- When the keyword table is queried, an automatic join is made with the
  dataset-keyword association table. So the user can query for all datasets
  with a given keyword, for example.
- Added keywords information to the documentation
- Can run `dregs show keywords` from CLI to display all pre-registered keywords

## Version 0.5.0

Separate out creation of production schema and non-production schema since,
under normal circumstances, there will be a single "real" production schema
(owner type == production only) but possibly multiple non-production schemas to
keep track of entries for the other owner types.  Add a field to the provenance
table so a schema can discover the name of its associated production schema and
form foreign key constraints correctly.

Bumped database version to 2.3.0.  This code requires database version >= 2.3.0

## Version 0.4.2

- Add check during dataset registration to raise an exception if the `root_dir`
  does not exist
- Add check before copying any data (i.e., `old_location != None`) that the
  user has write permission to the `root_dir` folder.

## Version 0.4.1

Add ability to register "external" datasets. For example datasets that are not
physically managed by the registry, or are offsite, therefore only a database
entry is created.

- Database version bumped to 2.2.0
- Added `location_type` column to `dataset` table (can be either "onsite",
  "external" or "dummy").
- Added `contact_email` and `url` column to `dataset` table. One of these is
  required when registering a `location_type="external"` dataset.
- Removed `is_external_link` column from `dataset` table as it is redundant.
- Renamed `execution.locale` to `execution.site` in the `execution` table.

## Version 0.4.0

Version 0.4.0 focuses around being able to manipulate data already within the
dataregistry, i.e., adding the ability to delete and modify previous datasets.

### Changelog for developers:

- `Registrar` now has a class for each table. They inherit from a `BaseTable`
  class, this means that shared functions, like deleting entries, are available
  for all tables. (#92)
- Working with tables via the python interface has slightly different syntax
  (see user changelog below). (#92)
- `is_valid` is removed as a `dataset` property. It has been replaced with
  `status` which is a bitmask (bit 0="valid", bit 1= "deleted" and bit
  2="archived"), so now datasets can a combination of multiple states. (#93)
- `archive_date`, `archive_path`, `delete_date`, `delete_uid` and `move_date`
  have been added as new `dataset` fields. (#93)
- Database version bumped to `2.0.1` (#93)
- `dataset` entries can be deleted (see below) (#94)
- The CI for the CLI is now pure Python (i.e., there is no more bash script to
  ingest dummy entries into the registry for testing).
- Can no longer "bump" a dataset that has a version suffix (trying to do so
  will raise an error). If a user wants to make a new version of a dataset with
  a suffix they can still do so by manually specifying the version and suffix
  (#97 ).
- Dataset entries can be modified (see below, #100)

### Changelog for users:

- All database tables (`dataset`, `execution`, etc) have a more universal
  syntax. The functionality is still accessed via the `Registrar` class, but
  now for example to register a dataset it's `Registrar.dataset.register()`,
  similarly for an execution `Registrar.execution.register()` (#92). The docs
  and tutorials have been updated (#95).
- `dataset` entries can now be deleted using the
  `Registrar.dataset.delete(dataset_id=...)` function. This will also delete
  the raw data within the `root_dir`. Note that the entry in the database will
  always remain (with an updated `status` field to indicate it has been
  deleted). (#94)
- Documentation has been updated to make things a bit clearer. Now split into
  more focused tutorials (#95).
- Certain dataset quantities can be modified after registration (#100).
  Documentation has been updated with examples.
