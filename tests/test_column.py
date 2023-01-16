from os import environ

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def dataframe(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = db.create_dataframe(rows=rows, column_names=["id"])
    t = t.save_as("const_dataframe", temp=True, column_names=["id"])
    return t


def test_expr_column_name(db: gp.Database, dataframe: gp.DataFrame):
    c = gp.col.Column("id", dataframe)
    assert c.name == "id"


def test_expr_column_str(db: gp.Database, dataframe: gp.DataFrame):
    c = gp.col.Column("id", dataframe)
    assert str(c) == '"const_dataframe"."id"'


def test_expr_column_str_in_query(db: gp.Database, dataframe: gp.DataFrame):
    c = gp.col.Column("id", dataframe)
    query = "select " + str(c) + " from " + c.dataframe.name
    tr = gp.DataFrame(query=query, db=db)
    for row in tr:
        assert list(row.keys()) == ["id"]
