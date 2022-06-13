import pytest

import greenplumpython as gp


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


@pytest.fixture
def table(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", temp=True, column_names=["id"])
    return t


def test_expr_column_name(db: gp.Database, table: gp.Table):
    c = gp.expr.Column("id", table)
    assert c.name == "id"


def test_expr_column_str(db: gp.Database, table: gp.Table):
    c = gp.expr.Column("id", table)
    assert str(c) == "const_table.id"


def test_expr_column_str_in_query(db: gp.Database, table: gp.Table):
    c = gp.expr.Column("id", table)
    query = "select " + str(c) + " from " + c.table.name
    tr = gp.Table(query=query, db=db)
    ret = tr.fetch()
    assert len(list(ret)) == 3
    for row in ret:
        keys = list(row.keys())
        assert len(keys) == 1
        assert "id" in keys[0]


def test_expr_column_rename(db: gp.Database, table: gp.Table):
    assert str(table["id"].rename("table_id")) == "const_table.id AS table_id"
