from functools import partial

import pytest

import greenplumpython as gp
from greenplumpython.builtin.function import generate_series
from tests import db


@pytest.fixture
def t(db: gp.Database):
    t = db.assign(id=lambda: generate_series(0, 9))
    return t


def test_order_by_slice(db: gp.Database, t: gp.Table):
    ret = t.order_by("id")[:5]
    assert len(list(ret)) == 5
    assert next(iter(ret))["id"] == 0
    assert next(ret)["id"] == 1
    assert next(ret)["id"] == 2
    assert next(ret)["id"] == 3
    assert next(ret)["id"] == 4


def test_order_by_slice_empty(db: gp.Database, t: gp.Table):
    ret = t.order_by("id")[:]
    assert len(list(ret)) == 10


def test_order_by_slice_desc(db: gp.Database, t: gp.Table):
    ret = t.order_by("id", ascending=False)[:5]
    assert len(list(ret)) == 5
    assert next(iter(ret))["id"] == 9
    assert next(ret)["id"] == 8
    assert next(ret)["id"] == 7
    assert next(ret)["id"] == 6
    assert next(ret)["id"] == 5


def test_order_by_slice_operator(db: gp.Database, t: gp.Table):
    ret = t.order_by("id", operator=">")[:3]
    assert len(list(ret)) == 3
    assert next(iter(ret))["id"] == 9
    assert next(ret)["id"] == 8
    assert next(ret)["id"] == 7


def test_order_by_slice_asc_operator(db: gp.Database, t: gp.Table):
    with pytest.raises(Exception) as exc_info:
        t.order_by("id", ascending=True, operator="<")
    assert str(exc_info.value).startswith(
        "Could not use 'ascending' and 'operator' at the same time to order by one column"
    )


def test_order_by_multiple_slice(db: gp.Database):
    # fmt: off
    rows = [(1, 2,), (1, 3,), (2, 2,), (3, 1,), (3, 4,)]
    # fmt: on
    t = gp.to_table(rows, db=db, column_names=["id", "num"])
    ret = t.order_by("id").order_by("num", ascending=False)[:5]
    assert len(list(ret)) == 5
    row = next(iter(ret))
    assert row["id"] == 1 and row["num"] == 3
    row = next(ret)
    row = next(ret)
    row = next(ret)
    row = next(ret)
    assert row["id"] == 3 and row["num"] == 1
    ret2 = t.order_by("num", ascending=False).order_by("id")[:5]
    assert len(list(ret)) == 5
    row = next(iter(ret2))
    assert row["id"] == 3 and row["num"] == 4
    row = next(ret2)
    row = next(ret2)
    row = next(ret2)
    row = next(ret2)
    assert row["id"] == 3 and row["num"] == 1


def test_order_by_nulls_last(db: gp.Database):
    # fmt: off
    rows = [(1, "Mona Lisa", None), (5, "The Birth of Venus", None),
            (3, "The Scream", 1893, ), (2, "The Starry Night", 1889,),
            (4, "The Night Watch", 1642,)]
    # fmt: on
    t = gp.to_table(rows, db=db, column_names=["id", "painting", "year"])
    ret = t.order_by("year", nulls_first=False)[:5]
    assert (
        next(iter(ret))["year"] is not None
        and next(ret)["year"] is not None
        and next(ret)["year"] is not None
    )
    assert next(ret)["year"] is None and next(ret)["year"] is None
