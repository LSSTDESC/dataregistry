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
