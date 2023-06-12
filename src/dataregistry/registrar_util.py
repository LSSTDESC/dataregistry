import os
import re
from sqlalchemy import MetaData, Table, Column, text, select

from dataregistry.db_basic import ownertypeenum

__all__ = ['make_version_string', 'parse_version_string', 'calculate_special',
           'form_dataset_path', 'get_directory_info']
VERSION_SEPARATOR = '.'
_nonneg_int_re = "0|[1-9][0-9]*"

def make_version_string(major, minor=0, patch=0, suffix=None):
    version = VERSION_SEPARATOR.join([major, minor, patch])
    if suffix:
        version = VERSION_SEPARATOR.join([version, suffix])
    return version

def parse_version_string(version, with_suffix=False):
    '''
    Return dict with keys major, minor, patch and (if present) suffix.
    with_suffix == False means version string *must not* include suffix
    with_suffix == True means it *may* have a suffix

    Returns a dict with keys 'major', 'minor', 'patch' and optionally 'suffix'
    '''
    # Perhaps better to do with regular expressions.  Or at least verify
    # that major, minor, patch are integers
    cmp = version.split(VERSION_SEPARATOR)
    if not with_suffix:
        if len(cmp) != 3:
            raise ValueError('Version string must have 3 components')
    else:
        if len(cmp) < 3 or len(cmp) > 4:
            raise ValueError('Version string must have 3 or 4 components')
    for c in cmp[0:3]:
        if not re.fullmatch(_nonneg_int_re, c):
            raise ValueError(f'Version component {c} is not non-negative int')
    d = {'major' : cmp[0]}
    d['minor'] = cmp[1]
    d['patch'] = cmp[2]

    if len(cmp) > 3:
        d['suffix'] = cmp[4]

    return d

## Alternatively, make this a method in a class so that the top-level
## root dir can be stored
def form_dataset_path(owner_type, owner, relative_path, root_dir=None):
    '''
    Return full absolute path if root_dir is specified, else path relative
    to the site-specific root
    Parameters
    ----------
    owner_type      of type ownertypeenum
    owner           string
    relative_path   string
    root_dir        string
    '''
    if owner_type == 'production':
        owner = 'production'
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

def calculate_special(name, v_string, v_suffix, dataset_table, engine):
    '''
    Utility to figure out what new version fields should be if caller
    to register supplies a special version string
    '''
    stmt = select(dataset_table.c["version_major","version_minor",
                                  "version_patch"])\
                                  .where(dataset_table.c.name == name)
    if v_suffix:
        stmt = stmt.where(dataset_table.c.version_suffix == v_suffix)
        stmt = stmt.order_by(dataset_table.c.version_major.desc())\
                   .order_by(dataset_table.c.version_minor.desc())\
                   .order_by(dataset_table.c.version_patch.desc())
    with engine.connect() as conn:
        try:
            result = conn.execute(stmt)
            conn.commit()
        except DBAPIError as e:
            print('Original error:')
            print(e.StatementError.orig)
            return None
        r = result.fetchone()
        if not r:
            old_major = 0
            old_minor = 0
            old_patch = 0
        else:
            old_major = int(r[0])
            old_minor = int(r[1])
            old_patch = int(r[2])
    v_fields = {'major' : old_major, 'minor' : old_minor, 'patch' : old_patch}
    v_fields[v_string] = v_fields[v_string] + 1
    return v_fields
