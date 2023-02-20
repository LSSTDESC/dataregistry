import os
from dataregistry.db_basic import ownertypeenum

__all__ = ['make_version_string', 'parse_version_string']
VERSION_SEPARATOR = '.'
def make_version_string(major, minor=0, patch=0, suffix=None):
    version = VERSION_SEPARATOR.join([major, minor, patch])
    if suffix:
        version = VERSION_SEPARATOR.join([version, suffix])
    return version

def parse_version_string(version):
    '''
    Return dict with keys major, minor, patch and (if present) suffix
    '''
    # Perhaps better to do with regular expressions.  Or at least verify
    # that major, minor, patch are integers
    cmp = version.split(_SEPARATOR, maxsplit=3)
    d = {'major' : cmp[0]}
    if len(cmp) > 1:
        d['minor'] = cmp[1]
    else:
        d['minor'] = 0
    if len(cmp) > 2:
        d['patch'] = cmp[2]
    else:
        d['patch'] = 0
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
