import pytest

import greenplumpython as gp
import greenplumpython.builtin.function as F
from tests import db


def test_builtin_func_call(db: gp.Database):
    rows = [(i,) for i in range(10)]
    t = gp.to_table(rows, db=db, column_names=["a"])
    result = list(t.group_by().assign(count=lambda t: F.count(t["a"])).fetch())
    assert len(result) == 1
    assert result[0]["count"] == 10


def test_builtin_func_apply(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = list(
        gp.to_table(rows, db=db, column_names=["a"])
        .group_by()
        .assign(count=lambda t: F.count(t["a"]))
        .fetch()
    )
    assert len(result) == 1
    assert result[0]["count"] == 10


def test_builtin_func_no_arg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    result = list(
        gp.to_table(rows, db=db, column_names=["a"])
        .group_by()
        .assign(count=lambda _: F.count())
        .fetch()
    )
    assert len(result) == 1
    assert result[0]["count"] == 10
