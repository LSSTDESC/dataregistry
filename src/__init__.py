def find_version():
    """Find the version"""
    # setuptools_scm should install a file _version alongside this one.
    from . import _version
    return _version.version

try:
    __version__ = find_version()
except ImportError:
    __version__ = "unknown"
