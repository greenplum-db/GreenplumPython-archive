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


def test_table_getitem_str(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db)
    t = t.save_as("const_table", column_names=["id"], temp=True)
    c = t["id"]
    assert str(c) == (t.name + ".id")


def test_table_getitem_slice(db: gp.Database):
    generate_series = gp.function("generate_series", db)
    t = (
        generate_series(0, 9, as_name="id")
        .to_table()
        .save_as("temp_table", column_names=["id"], temp=True)
    )
    assert len(list(t[:2].fetch())) == 2
    assert len(list(t[2:].fetch())) == 8
    assert len(list(t[2:5].fetch())) == 3
