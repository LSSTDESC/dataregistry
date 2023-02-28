try:
    # For Python >= 3.8
    from importlib import metadata
except ImportError:
    # For Python < 3.8
    import importlib_metadata as metadata

__version__ = metadata.version("dataregistry")

from .db_basic import *
from .registrar import *
