from os import environ

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def dataframe(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = db.create_dataframe(rows=rows, column_names=["id"])
    t = t.save_as(temp=True, column_names=["id"])
    return t


def test_expr_column_name(db: gp.Database, dataframe: gp.DataFrame):
    c = gp.col.Column("id", dataframe)
    assert c._name == "id"


def test_table_with_schema(db: gp.Database):
    pg_class = db.create_dataframe(table_name="pg_class", schema="pg_catalog")
    result = pg_class.order_by("oid")[:1][["oid"]]
    assert result["oid"] == 112
