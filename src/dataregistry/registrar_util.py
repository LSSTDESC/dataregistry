import os
import re
from sqlalchemy import MetaData, Table, Column, text, select

from dataregistry.db_basic import ownertypeenum

__all__ = [
    "_parse_version_string",
    "_bump_version",
    "_form_dataset_path",
    "get_directory_info",
    "_name_from_relpath",
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


def _form_dataset_path(owner_type, owner, relative_path, root_dir=None):
    """
    Construct full (or relative) path to dataset in the data registry.

    Path will have the format:
        <root_dir>/<owner_type>/<owner>/<relative_path>

    Parameters
    ----------
    owner_type : ownertypeenum
        Type of dataset
    owner : str
        Owner of dataset
    relative_path : str
        Relative path within the data registry
    root_dir : str
        Root directory of data registry

    Returns
    -------
    to_return : str
        Full path of dataset in the data registry
    """
    if owner_type == "production":
        owner = "production"
    to_return = os.path.join(owner_type, owner, relative_path)
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
