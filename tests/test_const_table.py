import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="postgres")
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
    t = t.save_as("const_table", column_names=["id"])
    c = t["id"]
    assert str(c) == (t.name + ".id")


def test_table_like(db: gp.Database):
    rows = [("'aaa'",), ("'bba'",), ("'acc'",)]
    t = gp.values(rows, db=db).save_as("temp2", column_names=["id"])
    result = t['id'].like("a%").fetch()
    assert len(list(result)) == 2
    result = t['id'].like("%a").fetch()
    assert len(list(result)) == 2
