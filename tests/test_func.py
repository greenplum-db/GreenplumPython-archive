import inspect

import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_plain_func(db: gp.Database):
    version = gp.function("version", db)
    for row in version().to_table().fetch():
        assert "Greenplum" in row["version"]


def test_set_returning_func(db: gp.Database):
    generate_series = gp.function("generate_series", db)
    results = generate_series(0, 9, as_name="id").to_table().fetch()
    assert sorted([row["id"] for row in results]) == list(range(10))


def test_create_func(db: gp.Database):
    @gp.create_function(db)
    def add(a: int, b: int) -> int:
        return a + b

    for row in add(1, 2, as_name="result").to_table().fetch():
        assert row["result"] == 1 + 2
        assert row["result"] == inspect.unwrap(add)(1, 2)


def test_create_func_multiline(db: gp.Database):
    @gp.create_function(db)
    def max(a: int, b: int) -> int:
        if a > b:
            return a
        else:
            return b

    for row in max(1, 2, as_name="result").to_table().fetch():
        assert row["result"] == 2
        assert row["result"] == inspect.unwrap(max)(1, 2)
