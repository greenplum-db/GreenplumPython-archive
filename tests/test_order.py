from functools import partial

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def t(db: gp.Database):
    generate_series = gp.function("generate_series")
    t = generate_series(0, 9, as_name="id", db=db).to_table()
    return t


def test_order_by_head(db: gp.Database, t: gp.Table):
    ret = list(t.order_by(t["id"]).head(5).fetch())
    assert len(ret) == 5
    assert ret[0]["id"] == 0
    assert ret[4]["id"] == 4


def test_order_by_head_desc(db: gp.Database, t: gp.Table):
    ret = list(t.order_by(t["id"], ascending=False).head(5).fetch())
    assert len(ret) == 5
    assert ret[0]["id"] == 9
    assert ret[4]["id"] == 5


def test_order_by_head_operator(db: gp.Database, t: gp.Table):
    ret = list(t.order_by(t["id"], operator=">").head(3).fetch())
    assert len(ret) == 3
    assert ret[0]["id"] == 9
    assert ret[2]["id"] == 7


def test_order_by_head_asc_operator(db: gp.Database, t: gp.Table):
    with pytest.raises(Exception) as exc_info:
        t.order_by(t["id"], ascending=True, operator="<")
    assert str(exc_info.value).startswith(
        "Could not use 'ascending' and 'operator' at the same time to order by one column"
    )


def test_order_by_multiple_head(db: gp.Database):
    # fmt: off
    rows = [(1, 2,), (1, 3,), (2, 2,), (3, 1,), (3, 4,)]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "num"])
    ret = list(t.order_by(t["id"]).order_by(t["num"], ascending=False).head(5).fetch())
    assert len(ret) == 5
    assert ret[0]["id"] == 1 and ret[0]["num"] == 3
    assert ret[4]["id"] == 3 and ret[4]["num"] == 1
    ret2 = list(t.order_by(t["num"], ascending=False).order_by(t["id"]).head(5).fetch())
    assert len(ret) == 5
    assert ret2[0]["id"] == 3 and ret2[0]["num"] == 4
    assert ret2[4]["id"] == 3 and ret2[4]["num"] == 1


def test_order_by_nulls_last(db: gp.Database):
    # fmt: off
    rows = [(1, "Mona Lisa", None), (5, "The Birth of Venus", None),
            (3, "The Scream", 1893, ), (2, "The Starry Night", 1889,),
            (4, "The Night Watch", 1642,)]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "painting", "year"])
    ret = list(t.order_by(t["year"], nulls_first=False).head(5).fetch())
    assert ret[0]["year"] is not None and ret[1]["year"] is not None and ret[2]["year"] is not None
    assert ret[3]["year"] is None and ret[4]["year"] is None
