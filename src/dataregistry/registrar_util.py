import hashlib
import os
import re
from sqlalchemy import MetaData, Table, Column, text, select
from shutil import copyfile, copytree, rmtree

__all__ = [
    "_parse_version_string",
    "_bump_version",
    "_form_dataset_path",
    "get_directory_info",
    "_name_from_relpath",
    "_copy_data",
]
VERSION_SEPARATOR = "."
_nonneg_int_re = "0|[1-9][0-9]*"


def _parse_version_string(version, with_suffix=False):
    """
    Parase a version string into its components.

    Parameters
    ----------
    version : str
        Version string
    with_suffix : bool
        False means version string *must not* include suffix
        True means it *may* have a suffix

    Returns
    -------
    d : dict
        Dict with keys "major", "minor", "patch" and optionally "suffix"
    """

    cmp = version.split(VERSION_SEPARATOR)
    if not with_suffix:
        if len(cmp) != 3:
            raise ValueError("Version string must have 3 components")
    else:
        if len(cmp) < 3 or len(cmp) > 4:
            raise ValueError("Version string must have 3 or 4 components")
    for c in cmp[0:3]:
        if not re.fullmatch(_nonneg_int_re, c):
            raise ValueError(f"Version component {c} is not non-negative int")
    d = {"major": cmp[0]}
    d["minor"] = cmp[1]
    d["patch"] = cmp[2]

    if len(cmp) > 3:
        d["suffix"] = cmp[3]

    return d


def _form_dataset_path(owner_type, owner, relative_path, schema=None, root_dir=None):
    """
    Construct full (or relative) path to dataset in the data registry.

    When schema and root_dir are not None, the full path is returned:
        <root_dir>/<schema>/<owner_type>/<owner>/<relative_path>

    When schema and root_dir are ommited, the relative path is returned:
        <owner_type>/<owner>/<relative_path>

    Parameters
    ----------
    owner_type : str
        Type of dataset
    owner : str
        Owner of dataset
    relative_path : str
        Relative path within the data registry
    schema : str, optional
        Schema we are connected to
    root_dir : str, optional
        Root directory of data registry
    dialect : str, optional
        SQL dialect, e.g postgres or sqlite

    Returns
    -------
    to_return : str
        Full (or relative) path of dataset in the data registry
    """

    if owner_type == "production":
        owner = "production"
    to_return = os.path.join(owner_type, owner, relative_path)
    if schema:
        to_return = os.path.join(schema, to_return)
    if root_dir:
        to_return = os.path.join(root_dir, to_return)
    return to_return


def get_directory_info(path):
    """
    Get the total disk space used by a directory and the total number of files
    in the directory (includes subdirectories):

    Parameters
    ----------
    path : str
        Location of directory

    Returns
    -------
    num_files : int
        Total number of files in dir (including subdirectories)
    total_size : float
        Total disk space (in bytes) used by directory (including subdirectories)
    """

    num_files = 0
    total_size = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                num_files += 1
                total_size += entry.stat().st_size
            elif entry.is_dir():
                subdir_num_files, subdir_total_size = get_directory_info(entry.path)
                num_files += subdir_num_files
                total_size += subdir_total_size
    return num_files, total_size


def _bump_version(name, v_string, v_suffix, dataset_table, engine):
    """
    Bump version of dataset automatically if user has supplied a special
    version string during register.

    Parameters
    ----------
    name : str
        Name of the dataset
    v_string : str
        Special version string "major", "minor", "patch"
    version_suffix : str
        Dataset version suffix
    dataset_table : SQLAlchemy Table object
    engine : SQLAlchemy Engine object

    Returns
    -------
    v_fields : dict
        Updated version dict with keys "major", "minor", "patch"
    """

    # Find the previous dataset based on the name and version suffix
    stmt = select(
        dataset_table.c["version_major", "version_minor", "version_patch"]
    ).where(dataset_table.c.name == name)
    if v_suffix:
        stmt = stmt.where(dataset_table.c.version_suffix == v_suffix)
        stmt = (
            stmt.order_by(dataset_table.c.version_major.desc())
            .order_by(dataset_table.c.version_minor.desc())
            .order_by(dataset_table.c.version_patch.desc())
        )
    with engine.connect() as conn:
        result = conn.execute(stmt)
        conn.commit()
        r = result.fetchone()
        if not r:
            old_major = 0
            old_minor = 0
            old_patch = 0
        else:
            old_major = int(r[0])
            old_minor = int(r[1])
            old_patch = int(r[2])

    # Add 1 to the relative version part.
    v_fields = {"major": old_major, "minor": old_minor, "patch": old_patch}
    v_fields[v_string] = v_fields[v_string] + 1

    # Reset fields as needed
    if v_string == "minor":
        v_fields["patch"] = 0
    if v_string == "major":
        v_fields["patch"] = 0
        v_fields["minor"] = 0

    return v_fields


def _name_from_relpath(relative_path):
    """
    Scrape the dataset name from the relative path.

    We use this when the dataset name is not explicitly defined, and we take it
    from the final directory if path.

        e.g, /root/to/dataset/dir would return "dir"

        Parameters
        ----------
        relative_path : str
                Path to dataset (can be relative or absolute)

        Returns
        -------
        name : str
                Scraped name of dataset
    """

    relpath = relative_path
    if relative_path.endswith("/"):
        relpath = relative_path[:-1]
    base = os.path.basename(relpath)
    if "." in base:
        cmp = base.split(".")
        name = ".".join(cmp[:-1])
    else:
        name = base

    return name


def _copy_data(dataset_organization, source, dest, do_checksum=True):
    """
    Copy data from one location to another (for ingesting directories and files
    into the `root_dir` shared space.

    Note prior to this, in `_handle_data`, it has already been check that
    `source` exists, so we do not have to check again.

    To ensure robustness, if overwriting data, the original file/folder is
    moved to a temporary location, then deleted if the copy was successful. If
    the copy was not successful the backup is renamed back.

    For individual files a checksum validation can be performed if
    `do_checksum=True`, there is no such check for directories.

    Parameters
    ----------
    dataset_organization : str
        The dataset organization, either "file" or "directory"
    source : str
        Path of source file or directory
    dest : str
        Destination we are copying to
    do_checksum : bool
        When overwriting files, do a checksum with the old and new file
    """

    def _compute_checksum(file_path):
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    temp_dest = dest + "_DATAREG_backup"

    try:
        # Backup original before copy
        if os.path.exists(dest):
            os.rename(dest, temp_dest)

        # Create any intervening directories
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        # Copy a single file
        if dataset_organization == "file":
            copyfile(source, dest)

            # Checksums on the files
            if do_checksum and os.path.exists(temp_dest):
                cs_dest = _compute_checksum(dest)
                cs_dest_backup = _compute_checksum(temp_dest)

                if cs_dest != cs_dest_backup:
                    raise Exception("Checksum with backup failed")

        # Copy a single directory (and subdirectories)
        elif dataset_organization == "directory":
            copytree(source, dest, copy_function=copyfile)

        # If successful, delete the backup
        if os.path.exists(temp_dest):
            if dataset_organization == "file":
                os.remove(temp_dest)
            else:
                rmtree(temp_dest)

    except Exception as e:
        if os.path.exists(temp_dest):
            if os.path.exists(dest):
                rmtree(dest)
            os.rename(temp_dest, dest)

        print(
            f"Something went wrong during data copying, aborting."
            "Note an entry in the registry database will still have"
            "been created"
        )

        raise Exception(e)
