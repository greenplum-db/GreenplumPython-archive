import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_const_table(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", column_names=["id"], temp=True)
    assert sorted([tuple(row.values()) for row in t.fetch()]) == sorted(rows)

    t_cols = t.column_names().fetch()
    assert len(list(t_cols)) == 1
    for row in t_cols:
        assert row["column_name"] == "id"


def test_table_getitem(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", temp=True, column_names=["id"])
    c = t["id"]
    assert str(c) == (t.name + ".id")


def test_table_join_eq(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t1 = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id"])
    t2 = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id"])
    ret = t1.join(t2, t1["id"] == t2["id"]).fetch()
    assert len(list(ret)) == 3


def test_table_join_lt(db: gp.Database):
    rows1 = [(1,), (2,), (3,)]
    rows2 = [(2,), (3,), (4,)]
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["a"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["b"])
    ret = t1.join(t2, t1["a"] < t2["b"], how="left").fetch()
    assert len(list(ret)) == 6


def test_table_join_target(db: gp.Database):
    rows1 = [(1, "'a1'",), (2, "'b1'",), (3, "'c1'",)]
    rows2 = [(1, "'a2'",), (2, "'b2'",), (3, "'c2'",)]
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "n1"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["id2", "n2"])
    ret = t1.join(t2, t1["id1"] == t2["id2"], how="left",
                  target_list=[t1["id1"], t1["n1"], t2["n2"]]).fetch()
    assert "id2" not in list(list(ret)[0].keys())
