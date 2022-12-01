import inspect
from typing import List

import pytest

import greenplumpython as gp
from greenplumpython.type import to_pg_type
from tests import db


def test_type_cast(db: gp.Database):
    rows = [(i,) for i in range(10)]
    series = gp.to_table(rows, db, column_names=["val"]).save_as("series", temp=True)
    regclass = gp.get_type("regclass")
    table_name = series.assign(table_name=lambda t: regclass(t["tableoid"]))
    for row in table_name:
        assert row["table_name"] == "series"


def test_type_create(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    type_name = to_pg_type(Person, db)
    assert isinstance(type_name, str)
