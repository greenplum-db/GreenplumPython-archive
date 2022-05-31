import pytest

import greenplumpython as gp


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_expr_bin_equal_int(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t = gp.values(rows, db=db).save_as("temp1", column_names=["id"])
    b1 = t["id"] == 2
    assert str(b1) == str(gp.expr.BinaryExpr("=", t["id"], 2))
    assert str(b1) == "temp1.id = 2"
    ret = t[b1].fetch()
    assert len(list(ret)) == 2


def test_expr_bin_equal_str(db: gp.Database):
    rows = [("'aa'",), ("'bb'",), ("'cc'",)]
    t = gp.values(rows, db=db).save_as("temp2", column_names=["id"])
    b2 = t["id"] == "aa"
    assert str(b2) == str(gp.expr.BinaryExpr("=", t["id"], "aa"))
    assert str(b2) == "temp2.id = 'aa'"
    ret = t[b2].fetch()
    assert len(list(ret)) == 1


def test_expr_bin_equal_none(db: gp.Database):
    rows = [("'aa'",), ("NULL",), ("'cc'",)]
    t = gp.values(rows, db=db).save_as("temp3", column_names=["id"])
    b3 = t["id"] == None
    assert str(b3) == str(gp.expr.BinaryExpr("is", t["id"], None))
    assert str(b3) == "temp3.id is NULL"
    ret = t[b3].fetch()
    assert len(list(ret)) == 1


# TODO : Add Query Condition Test for JOIN
def test_expr_bin_equal_2expr(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t1 = gp.values(rows, db=db).save_as("temp4", column_names=["id"])
    t2 = gp.values(rows, db=db).save_as("temp5", column_names=["id"])
    b4 = t1["id"] == t2["id"]
    assert str(b4) == str(gp.expr.BinaryExpr("=", t1["id"], t2["id"]))
    assert str(b4) == "temp4.id = temp5.id"


def test_expr_bin_equal_bool(db: gp.Database):
    rows = [("True",), ("False",), ("False",), ("True",)]
    t = gp.values(rows, db=db)
    t = t.save_as("temp1", column_names=["id"])
    c = t["id"]
    b5 = gp.expr.BinaryExpr("=", c, True)
    assert str(b5) == "temp1.id = TRUE"
    ret = t[b5].fetch()
    assert len(list(ret)) == 2
