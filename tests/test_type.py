import inspect
from typing import List

import pytest

import greenplumpython as gp
from greenplumpython.type import create_type
from tests import db


def test_type_cast(db: gp.Database):
    rows = [(i,) for i in range(10)]
    series = gp.to_table(rows, db, column_names=["val"]).save_as("series", temp=True)
    regclass = gp.get_type("regclass", db)
    table_name = series.assign(table_name=lambda t: regclass(t["tableoid"]))
    for row in table_name:
        assert row["table_name"] == "series"


def test_type_create(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    type_name = create_type(Person, db)
    assert isinstance(type_name, str)


def test_create_type_recursive(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    class Couple:
        _first_person: Person
        _second_person: Person

    def create_couple() -> Couple:
        return Couple()

    # FIXME : In this case, program will create twice Person type
    #         when creating Couple type with different type_name
    func_sig = inspect.signature(create_couple)
    create_type(func_sig.return_annotation, db)
