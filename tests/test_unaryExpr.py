import greenplumpython as gp
from tests import db


def test_expr_unary_not(db: gp.Database):
    rows = [(True,), (True,), (True,), (True,)]
    t = gp.to_table(rows, db=db, column_names=["id"])
    ret = t.assign(result=lambda t: ~t["id"])
    for row in ret:
        assert not row["result"]


def test_expr_unary_pos(db: gp.Database):
    rows = [(-1,), (-2,), (-3,), (-2,)]
    t = gp.to_table(rows, db=db, column_names=["id"])
    ret = t.assign(result=lambda t: +t["id"])
    for row in ret:
        assert row["result"] < 0


def test_expr_unary_neg(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t = gp.to_table(rows, db=db, column_names=["id"])
    ret = t.assign(result=lambda t: -t["id"])
    for row in ret:
        assert row["result"] < 0


def test_expr_unary_abs(db: gp.Database):
    rows = [(1,), (-2,), (-3,), (2,)]
    t = gp.to_table(rows, db=db, column_names=["id"])
    ret = t.assign(result=lambda t: abs(t["id"]))
    for row in ret:
        assert row["result"] > 0
