import greenplumpython as gp
import greenplumpython.builtin.function as F
from tests import db


def test_builtin_func_call(db: gp.Database):
    rows = [(i,) for i in range(10)]
    t = gp.values(rows, db=db, column_names=["a"])
    result = list(F.count(t["a"]).to_table().fetch())
    assert len(result) == 1
    assert result[0]["count"] == 10


def test_builtin_func_apply(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = list(gp.values(rows, db=db, column_names=["a"])["a"].apply(F.count).to_table().fetch())
    assert len(result) == 1
    assert result[0]["count"] == 10


def test_builtin_func_no_arg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = list(
        gp.values(rows, db=db, column_names=["a"]).apply(lambda _: F.count()).to_table().fetch()
    )
    assert len(result) == 1
    assert result[0]["count"] == 10
