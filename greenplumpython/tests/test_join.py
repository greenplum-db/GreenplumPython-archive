import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_table_inner_join_eq(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t1 = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id"])
    t2 = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id"])
    ret = t1.inner_join(t2, t1["id"] == t2["id"]).fetch()
    assert len(list(ret)) == 3


def test_table_left_join_lt(db: gp.Database):
    rows1 = [(1,), (2,), (3,)]
    rows2 = [(2,), (3,), (4,)]
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["a"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["b"])
    ret = t1.left_join(t2, t1["a"] < t2["b"]).fetch()
    assert len(list(ret)) == 6


def test_table_right_join_lt(db: gp.Database):
    rows1 = [(1,), (2,), (3,)]
    rows2 = [(3,), (4,), (5,)]
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["a"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["b"])
    ret = t1.left_join(t2, t1["a"] < t2["b"]).fetch()
    assert len(list(ret)) == 8


def test_table_full_join_target(db: gp.Database):
    # fmt: off
    rows1 = [(1, "'a1'",), (2, "'b1'",), (3, "'c1'",)]
    rows2 = [(1, "'a2'",), (2, "'b2'",), (3, "'c2'",)]
    # fmt: on
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "n1"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["id2", "n2"])
    ret = t1.full_outer_join(
        t2, t1["id1"] == t2["id2"], my_targets=["id1", "n1"], other_targets=["n2"]
    ).fetch()
    assert "id2" not in list(list(ret)[0].keys())
    assert list(list(ret)[0].keys()) == ["id1", "n1", "n2"]


def test_table_cross_join(db: gp.Database):
    # fmt: off
    rows1 = [(1, "'a1'",), (2, "'a2'",), (3, "'a3'",)]
    rows2 = [("'b1'",), ("'b2'",), ("'b3'",)]
    # fmt: on
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "n1"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["n1"])
    ret = t1.cross_join(
        t2, my_targets=["id1 AS t1_id1", "n1 AS t1_n1"], other_targets=["n1 AS t2_n1"]
    ).fetch()
    assert len(list(ret)) == 9
    assert "id1" not in list(list(ret)[0].keys())
    assert list(list(ret)[0].keys()) == ["t1_id1", "t1_n1", "t2_n1"]


def test_table_self_join(db: gp.Database):
    # fmt: off
    rows1 = [(1, "'a1'",), (2, "'a2'",), (3, "'a3'",)]
    # fmt: on
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "n1"])
    t2 = t1.as_name("temp2")
    ret = t1.inner_join(
        t2,
        t1["id1"] == t2["id1"],
        my_targets=["*"],
        other_targets=["id1 AS t2_id1", "n1 AS t2_n1"],
    ).fetch()
    assert len(list(ret)) == 3
    assert list(list(ret)[0].keys()) == ["id1", "n1", "t2_id1", "t2_n1"]


def test_table_join_save(db: gp.Database):
    # fmt: off
    rows1 = [(1, "'a1'",), (2, "'a2'",), (3, "'a3'",)]
    # fmt: on
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "n1"])
    t2 = t1.as_name("temp2")
    t_join = t1.inner_join(
        t2,
        t1["id1"] == t2["id1"],
        my_targets=["*"],
        other_targets=["id1 AS t2_id1", "n1 AS t2_n1"],
    )
    t_join.save_as("table_join")
    t_join_reload = gp.table("table_join", db=db)
    ret = t_join_reload.fetch()
    assert len(list(list(ret)[0].keys())) == 4
    assert list(list(ret)[0].keys()) == ["id1", "n1", "t2_id1", "t2_n1"]
    assert len(list(ret)) == 3
