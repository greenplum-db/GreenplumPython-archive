import pytest

import greenplumpython as gp
import greenplumpython.builtin.function as F
from tests import db


def test_builtin_func_call(db: gp.Database):
    rows = [(i,) for i in range(10)]
    t = gp.values(rows, db=db, column_names=["a"])
    result = list(F.count(t["a"]).to_table()._fetch())
    result_iter = iter(F.count(t["a"]).to_table())
    assert next(result_iter)["count"] == 10
    with pytest.raises(Exception) as exc_info:
        next(result_iter)

    assert str(exc_info.value) == "StopIteration: Reached last row of table!"


def test_builtin_func_apply(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = iter(gp.values(rows, db=db, column_names=["a"])["a"].apply(F.count).to_table())
    assert result.ndim == 1
    assert next(result)["count"] == 10


def test_builtin_func_no_arg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = iter(gp.values(rows, db=db, column_names=["a"]).apply(lambda _: F.count()).to_table())
    assert result.ndim == 1
    assert next(result)["count"] == 10
