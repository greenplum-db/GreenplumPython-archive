from os import environ

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def table(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.to_table(rows, db=db)
    t = t.save_as("const_table", temp=True, column_names=["id"])
    return t


def test_expr_column_name(db: gp.Database, table: gp.Table):
    c = gp.col.Column("id", table)
    assert c.name == "id"


def test_expr_column_str(db: gp.Database, table: gp.Table):
    c = gp.col.Column("id", table)
    assert str(c) == "const_table.id"


def test_expr_column_str_in_query(db: gp.Database, table: gp.Table):
    c = gp.col.Column("id", table)
    query = "select " + str(c) + " from " + c.table.name
    tr = gp.Table(query=query, db=db)
    for row in tr:
        keys = row.column_names()
        assert len(keys) == 1
        assert "id" in keys[0]
