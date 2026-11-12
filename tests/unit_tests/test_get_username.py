import os

import pytest
from dataregistry.db_basic import get_username

def test_username():
    """
    get_username function should always return the value of environment
    variable "USER" when the process was started up, even if someone
    mucked with it subsequently
    """
    orig = os.getenv('USER')

    assert orig == get_username()
    os.environ['USER'] = "nobody"

    assert os.getenv('USER') != orig
    assert orig == get_username()
