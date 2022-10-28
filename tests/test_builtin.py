import pytest

import greenplumpython as gp
import greenplumpython.builtin.function as F
from tests import db


def test_builtin_func_assign(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = (
        gp.to_table(rows, db=db, column_names=["a"])
        .group_by()
        .assign(count=lambda t: F.count(t["a"]))
    )
    assert len(list(result)) == 1
    assert next(iter(result))["count"] == 10


def test_builtin_func_assign_stop_iteration(db: gp.Database):
    rows = [(i,) for i in range(10)]
    t = gp.to_table(rows, db=db, column_names=["a"])
    result = t.group_by().assign(count=lambda t: F.count(t["a"]))
    assert (len(list(result))) == 1
    assert next(iter(result))["count"] == 10
    with pytest.raises(Exception) as exc_info:
        next(result)

    assert str(exc_info.value) == "StopIteration: Reached last row of table!"


def test_builtin_func_no_arg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = (
        gp.to_table(rows, db=db, column_names=["a"]).group_by().assign(count=lambda _: F.count())
    )
    assert len(list(result)) == 1
    assert next(iter(result))["count"] == 10


def test_builtin_func_apply(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = gp.to_table(rows, db=db, column_names=["a"]).apply(lambda _: F.count())
    assert len(list(result)) == 1
    assert next(iter(result))["count"] == 10
