# Data Registry CLI Cheat Sheet

## Overview

**CLI Name:** dregs\
**Version:** 1.9.0\
**Date:** August 2026\
**Description:** A command-line tool for querying and managing datasets in a data registry.

---

## Main Commands

| Command          | Description                            |
| ---------------- | -------------------------------------- |
| `dregs show`     | Show some properties                   |
| `dregs ls`       | List your entries in the data registry |
| `dregs path`     | Print the path to one dataset          |
| `dregs modify`   | Modify an entry in the database        |
| `dregs register` | Register a new entry to the database   |
| `dregs delete`   | Delete an entry in the database        |

---

NOTE: for the most up-to-date help information use command line help, e.g.

| Help command                    |
| ------------------------------- |
| `dregs ls --help`               |
| `dregs register --help`         |
| `dregs register dataset --help` |

## 🟦 Querying from the Command Line 🟦

### Listing Datasets

The `dregs ls` command lists all datasets in the connected namespace owned by the current user (`$USER`). Note only columns from the dataset table are returned.

| Command                    | Description                                  |
| -------------------------- | -------------------------------------------- |
| `dregs ls`                 | List all datasets owned by the current user. |
| `dregs ls --owner none`    | List all datasets in the namespace.          |
| `dregs ls --owner user123` | List datasets owned by `user123`.            |

### Selecting Specific Columns

| Command                                     | Description                  |
| ------------------------------------------- | ---------------------------- |
| `dregs ls --return-cols name version owner` | Show only specified columns. |

### Limiting Rows and Characters

| Command                   | Description                          |
| ------------------------- | ------------------------------------ |
| `dregs ls --max-rows 100` | Limit output to 100 rows.            |
| `dregs ls --max-chars 20` | Limit column width to 20 characters. |

### Filtering by Keyword

| Command                      | Description                               |
| ---------------------------- | ----------------------------------------- |
| `dregs ls --keyword science` | Filter datasets by the keyword `science`. |

### Printing One Dataset Path

Use `dregs path` to print the absolute path for one dataset by `dataset_id`.

| Command                                                 | Description                                                     |
| ------------------------------------------------------- | --------------------------------------------------------------- |
| `dregs path 1234`                                       | Print the absolute path for dataset id `1234`.                 |
| `dregs path 1234 --schema-mode working`                 | Resolve path in the working schema.                             |
| `dregs path 1234 --schema-mode production`              | Resolve path in the production schema.                          |
| `dregs path 1234 --root-dir /path/to/root --namespace desc` | Resolve path using an explicit root directory and namespace. |

---

## 🟩 Registering a Dataset 🟩

### Adding a New Dataset

To register a new dataset, use the following command:

```
dregs register dataset my_dataset 1.0.0 \
    --old-location /path/to/data \
    --owner myowner \
    --owner-type group \
    --description "My first dataset in the registry"
```

### Description of Options

| Option                              | Description                                                                 |
|-------------------------------------|-----------------------------------------------------------------------------|
| `my_dataset`                        | The name to register the dataset under.                                    |
| `1.0.0`                             | The version of the dataset following semantic versioning.                  |
| `--old-location /path/to`           | The absolute path to the existing dataset location. Data will be copied to the root directory. |
| `--owner myowner`                   | The owner of the dataset.                                                  |
| `--owner-type group`                | Specifies that the owner type is a group.                                  |
| `--description "My first dataset"`  | A human-readable description of the dataset.                               |

### Additional Properties

There are many other properties that can be set when registering a dataset. You can use:

```
dregs register dataset --help
```

to see all available options. We recommend being as detailed as possible when providing metadata.

## 🟨 Modifying a Dataset 🟨

### Updating Dataset Information

To modify an existing dataset (in this example modifying the description field), use a command like this:

```
dregs modify dataset 1234 \
    description \
    "Updated dataset description"
```

### Description of Options

| Option                                      | Description                                      |
| ------------------------------------------- | ------------------------------------------------ |
| `1234`                                      | The dataset ID of the dataset to be modified.    |
| `description`                               | Specifies which column in the dataset to modify. |
| `"Updated dataset description"`             | The new value to set for the specified column.   |

### Additional Properties

There are additional fields that can be modified. You can use:

```
dregs modify dataset --help
```

to see all available options.

---

## 🟥 Deleting a Dataset 🟥

### Removing a Dataset

To delete a dataset, use the following command:

```
dregs delete dataset my_dataset 1.0.0 \
    myowner \
    group
```

### Description of Options

| Option               | Description                               |
| -------------------- | ----------------------------------------- |
| `my_dataset`         | The name of the dataset to be deleted.    |
| `1.0.0`              | The version of the dataset to be deleted. |
| `myowner`            | The owner of the dataset.                 |
| `group`              | Specifies that the owner is a group.      |

### Confirmation

Deleting a dataset is irreversible. Ensure you have the correct details before running the command.
To see additional deletion options, use:

```
dregs delete dataset --help
```

---
