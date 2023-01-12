from functools import partial

import pytest

import greenplumpython as gp
from greenplumpython.builtin.function import generate_series
from tests import db


@pytest.fixture
def t(db: gp.Database):
    t = db.assign(id=lambda: generate_series(0, 9))
    return t


def test_order_by_slice(db: gp.Database, t: gp.DataFrame):
    ret = list(t.order_by("id")[:5])
    assert len(ret) == 5
    for i in range(5):
        assert ret[i]["id"] == i


def test_order_by_slice_all(db: gp.Database, t: gp.DataFrame):
    ret = t.order_by("id")[:]
    assert len(list(ret)) == 10


def test_order_by_slice_desc(db: gp.Database, t: gp.DataFrame):
    ret = list(t.order_by("id", ascending=False)[:5])
    assert len(ret) == 5
    for i in range(5):
        assert ret[i]["id"] == 10 - 1 - i


def test_order_by_slice_operator(db: gp.Database, t: gp.DataFrame):
    ret = list(t.order_by("id", operator=">")[:3])
    assert len(ret) == 3
    for i in range(3):
        assert ret[i]["id"] == 10 - 1 - i


def test_order_by_slice_asc_operator(db: gp.Database, t: gp.DataFrame):
    with pytest.raises(Exception) as exc_info:
        t.order_by("id", ascending=True, operator="<")
    assert str(exc_info.value).startswith(
        "Could not use 'ascending' and 'operator' together to order by one column"
    )


def test_multiple_order_by_slice(db: gp.Database):
    rows = [(1, 2), (1, 3), (2, 2), (3, 1), (3, 4)]
    t = db.create_dataframe(rows=rows, column_names=["id", "num"])
    ret = t.order_by("id").order_by("num", ascending=False)[:5]
    assert len(list(ret)) == 5
    row = next(iter(ret))
    assert row["id"] == 1 and row["num"] == 3
    ret2 = t.order_by("num", ascending=False).order_by("id")[:5]
    assert len(list(ret)) == 5
    row = next(iter(ret2))
    assert row["id"] == 3 and row["num"] == 4


def test_order_by_nulls_last(db: gp.Database):
    # fmt: off
    rows = [(1, "Mona Lisa", None), (5, "The Birth of Venus", None),
            (3, "The Scream", 1893, ), (2, "The Starry Night", 1889,),
            (4, "The Night Watch", 1642,)]
    # fmt: on
    t = db.create_dataframe(rows=rows, column_names=["id", "painting", "year"])
    ret = list(t.order_by("year", nulls_first=False)[:5])
    assert ret[-1]["year"] is None and ret[-2]["year"] is None
