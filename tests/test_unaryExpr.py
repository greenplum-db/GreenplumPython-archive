import pytest

import greenplumpython as gp


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_expr_unary_not(db: gp.Database):
    rows = [("True",), ("False",), ("True",), ("True",)]
    t = gp.values(rows, db=db).save_as("temp1", column_names=["id"], temp=True)
    b1 = ~t["id"]
    assert str(b1) == 'NOT(temp1.id) AS "Not(temp1.id)"'
    ret = list(t[["id", str(b1)]].fetch())
    for row in ret:
        bool_id = row["id"]
        bool_id_not = row["Not(temp1.id)"]
        assert bool_id ^ bool_id_not


def test_expr_unary_pos(db: gp.Database):
    rows = [(-1,), (-2,), (-3,), (-2,)]
    t = gp.values(rows, db=db).save_as("temp2", column_names=["id"], temp=True)
    b2 = +t["id"]
    assert str(b2) == '+temp2.id AS "+temp2.id"'
    ret = list(t[["id", str(b2)]].fetch())
    for row in ret:
        assert row["+temp2.id"] == +(row["id"])


def test_expr_unary_neg(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t = gp.values(rows, db=db).save_as("temp3", column_names=["id"], temp=True)
    b3 = -t["id"]
    assert str(b3) == '-temp3.id AS "-temp3.id"'
    ret = list(t[["id", str(b3)]].fetch())
    for row in ret:
        assert row["-temp3.id"] == -(row["id"])


def test_expr_unary_abs(db: gp.Database):
    rows = [(1,), (-2,), (-3,), (2,)]
    t = gp.values(rows, db=db).save_as("temp4", column_names=["id"], temp=True)
    b4 = abs(t["id"])
    assert str(b4) == 'ABS(temp4.id) AS "Abs(temp4.id)"'
    ret = list(t[["id", str(b4)]].fetch())
    for row in ret:
        assert row["Abs(temp4.id)"] == abs(row["id"])
