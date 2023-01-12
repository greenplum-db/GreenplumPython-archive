import pytest

import greenplumpython as gp
import greenplumpython.builtin.function as F
from tests import db


def test_builtin_func_assign(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = (
        db.create_dataframe(rows=rows, column_names=["a"])
        .group_by()
        .assign(count=lambda t: F.count(t["a"]))
    )
    assert len(list(result)) == 1
    assert next(iter(result))["count"] == 10


def test_builtin_func_assign_stop_iteration(db: gp.Database):
    rows = [(i,) for i in range(10)]
    t = db.create_dataframe(rows=rows, column_names=["a"])
    result = t.group_by().assign(count=lambda t: F.count(t["a"]))
    assert (len(list(result))) == 1
    df_iter = iter(result)
    assert next(df_iter)["count"] == 10

    with pytest.raises(StopIteration) as exc_info:
        next(df_iter)
    assert exc_info.errisinstance(StopIteration)


def test_builtin_func_no_arg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = (
        db.create_dataframe(rows=rows, column_names=["a"])
        .group_by()
        .assign(count=lambda _: F.count())
    )
    assert len(list(result)) == 1
    assert next(iter(result))["count"] == 10


def test_builtin_func_apply(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = db.create_dataframe(rows=rows, column_names=["a"]).apply(lambda _: F.count())
    assert len(list(result)) == 1
    assert next(iter(result))["count"] == 10
