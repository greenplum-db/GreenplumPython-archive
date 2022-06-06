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


# TODO: Test other data types
def test_create_func(db: gp.Database):
    @gp.create_function
    def add(a: int, b: int) -> int:
        return a + b

    for row in add(1, 2, as_name="result", db=db).to_table().fetch():
        assert row["result"] == 1 + 2
        assert row["result"] == inspect.unwrap(add)(1, 2)


def test_create_func_multiline(db: gp.Database):
    @gp.create_function
    def my_max(a: int, b: int) -> int:
        if a > b:
            return a
        else:
            return b

    for row in my_max(1, 2, as_name="result", db=db).to_table().fetch():
        assert row["result"] == max(1, 2)
        assert row["result"] == inspect.unwrap(my_max)(1, 2)


# fmt: off
def test_create_func_tab_indent(db: gp.Database):
	@gp.create_function
	def my_min(a: int, b: int) -> int:
		if a < b:
			return a
		else:
			return b

	for row in my_min(1, 2, as_name="result", db=db).to_table().fetch():
		assert row["result"] == min(1, 2)
		assert row["result"] == inspect.unwrap(my_min)(1, 2)
# fmt: on


def test_func_on_one_column(db: gp.Database):
    rows = [(i,) for i in range(-10, 0)]
    series = gp.values(rows, db=db, column_names=["id"])
    abs = gp.function("abs", db=db)
    results = abs(series["id"]).to_table().fetch()
    assert sorted([row["abs"] for row in results]) == list(range(1, 11))


def test_func_on_multi_columns(db: gp.Database):
    @gp.create_function
    def multiply(a: int, b: int) -> int:
        return a * b

    rows = [(i, i) for i in range(10)]
    series = gp.values(rows, db=db, column_names=["a", "b"])
    results = multiply(series["a"], series["b"], as_name="result").to_table().fetch()
    assert sorted([row["result"] for row in results]) == [i * i for i in range(10)]
