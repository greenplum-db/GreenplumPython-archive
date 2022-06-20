import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


@pytest.fixture
def t(db: gp.Database):
    generate_series = gp.function("generate_series", db)
    t = (
        generate_series(0, 9, as_name="id")
        .to_table()
        .save_as("temp_table", temp=True, column_names=["id"])
    )
    return t


def test_const_table(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", column_names=["id"], temp=True)
    assert sorted([tuple(row.values()) for row in t.fetch()]) == sorted(rows)

    t_cols = t.column_names().fetch()
    assert len(list(t_cols)) == 1
    for row in t_cols:
        assert row["column_name"] == "id"


def test_table_getitem_str(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", temp=True, column_names=["id"])
    c = t["id"]
    assert str(c) == (t.name + ".id")


def test_table_order_by_str(db: gp.Database, t: gp.Table):
    ret = t.order_by(["id"]).fetch()
    prev = -1
    for row in list(ret):
        assert row["id"] == prev + 1
        prev += 1


def test_table_order_by_desc(db: gp.Database, t: gp.Table):
    ret = t.order_by({"id": "DESC"}).fetch()
    prev = 10
    for row in list(ret):
        assert row["id"] == prev - 1
        prev -= 1


def test_table_order_by_multiple(db: gp.Database):
    # fmt: off
    rows = [(1, 2,), (1, 3,), (2, 2,), (3, 1,), (3, 4,)]
    # fmt: on
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", temp=True, column_names=["id", "num"])
    ret = t.order_by({"id": "ASC", "num": "DESC"}).fetch()
    prev_id = 0
    prev_num = 5
    for row in list(ret):
        assert row["id"] >= prev_id
        if row["id"] == prev_id:
            assert row["num"] <= prev_num
        prev_id = row["id"]
        prev_num = row["num"]


def test_table_getitem_slice_limit(db: gp.Database, t: gp.Table):
    ret = list(t.order_by(["id"])[:2].fetch())
    assert len(ret) == 2
    assert ret[0]["id"] == 0
    assert ret[1]["id"] == 1


def test_table_getitem_slice_offset(db: gp.Database, t: gp.Table):
    ret = list(t.order_by(["id"])[7:].fetch())
    assert len(ret) == 3
    assert ret[0]["id"] == 7
    assert ret[1]["id"] == 8
    assert ret[2]["id"] == 9


def test_table_getitem_slice_off_limit(db: gp.Database, t: gp.Table):
    ret = list(t.order_by(["id"])[2:5].fetch())
    assert len(ret) == 3
    assert ret[0]["id"] == 2
    assert ret[1]["id"] == 3
    assert ret[2]["id"] == 4
