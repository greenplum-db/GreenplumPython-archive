import pytest

import greenplumpython as gp
from tests import db


def test_pickler_option(server_use_pickler: bool):
    assert server_use_pickler == True


from dataclasses import dataclass


@pytest.mark.requires_pickler_on_server
def test_pickler_outside_class(db: gp.Database):
    @dataclass
    class Int:
        val: int

    @gp.create_function
    def add_one(i: int) -> Int:
        return Int(i + 1)

    for row in db.apply(lambda: add_one(1), expand=True):
        assert row["val"] == 2
