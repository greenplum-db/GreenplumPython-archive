import pytest

import greenplumpython as gp


def test_option(server_use_pickler: bool):
    assert server_use_pickler == True
