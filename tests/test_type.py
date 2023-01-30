import inspect
from typing import List

import pytest

import greenplumpython as gp
from greenplumpython.type import to_pg_type
from tests import db


def test_type_cast(db: gp.Database):
    rows = [(i,) for i in range(10)]
    series = db.create_dataframe(rows=rows, column_names=["val"]).save_as(
        "series", column_names=["val"], temp=True
    )
    regclass = gp.get_type("regclass")
    dataframe_name = series.assign(dataframe_name=lambda t: regclass(t["tableoid"]))
    for row in dataframe_name:
        assert row["dataframe_name"] == "series"


def test_type_create(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    type_name = to_pg_type(Person, db)
    assert isinstance(type_name, str)
