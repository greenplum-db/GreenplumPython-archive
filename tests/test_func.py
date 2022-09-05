import inspect
from os import environ
from typing import List

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def series(db: gp.Database):
    rows = [(i, i) for i in range(10)]
    return gp.values(rows, db=db, column_names=["a", "b"])


def test_plain_func(db: gp.Database):
    version = gp.function("version")
    for row in version(db=db).to_table().fetch():
        assert "Greenplum" in row["version"] or row["version"].startswith("PostgreSQL")


def test_set_returning_func(db: gp.Database):
    generate_series = gp.function("generate_series")
    results = generate_series(0, 9, as_name="id", db=db).to_table().fetch()
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
    abs = gp.function("abs")
    results = abs(series["id"], db=db).to_table().fetch()
    assert sorted([row["abs"] for row in results]) == list(range(1, 11))


def test_func_on_multi_columns(db: gp.Database, series: gp.Table):
    @gp.create_function
    def multiply(a: int, b: int) -> int:
        return a * b

    results = multiply(series["a"], series["b"], as_name="result").to_table().fetch()
    assert sorted([row["result"] for row in results]) == [i * i for i in range(10)]


def test_func_on_more_than_one_table(db: gp.Database):
    div = gp.function("div")
    rows = [(1,) for _ in range(10)]
    t1 = gp.values(rows, db=db, column_names=["i"])
    t2 = gp.values(rows, db=db, column_names=["i"])
    with pytest.raises(Exception) as exc_info:
        div(t1["i"], t2["i"], db=db)
    # FIXME: Create more specific exception classes and remove this
    assert "Cannot pass arguments from more than one tables" == str(exc_info.value)


def test_create_func_optional_params_replace(db: gp.Database, series: gp.Table):
    @gp.create_function
    def multiply(a: int, b: int) -> int:
        return a * b

    results = multiply(series["a"], series["b"], as_name="result").to_table().fetch()
    assert sorted([row["result"] for row in results]) == [i * i for i in range(10)]

    @gp.create_function(replace_if_exists=True)
    def multiply(a: int, b: int) -> int:
        return a * b * 2

    results = multiply(series["a"], series["b"], as_name="result").to_table().fetch()
    assert sorted([row["result"] for row in results]) == [i * i * 2 for i in range(10)]
    assert sorted([row["result"] for row in results]) == [
        inspect.unwrap(multiply)(i, i) for i in range(10)
    ]

    @gp.create_function(replace_if_exists=False)
    def multiply(a: int, b: int) -> int:
        return a * b * 2

    with pytest.raises(Exception) as exc_info:
        multiply(series["a"], series["b"], as_name="result").to_table().fetch()
    assert 'function "multiply" already exists with same argument types\n' == str(exc_info.value)


def test_create_func_optional_params_name(db: gp.Database, series: gp.Table):
    @gp.create_function
    def multiply(a: int, b: int) -> int:
        return a * b

    results = multiply(series["a"], series["b"], as_name="result").to_table().fetch()
    assert sorted([row["result"] for row in results]) == [i * i for i in range(10)]

    @gp.create_function(name="multiply", replace_if_exists=True)
    def multiply2(a: int, b: int) -> int:
        return a * b * 2

    rows = [(i, i) for i in range(10)]
    series = gp.values(rows, db=db, column_names=["a", "b"])
    results = multiply2(series["a"], series["b"]).to_table().fetch()
    assert sorted([row["multiply"] for row in results]) == [i * i * 2 for i in range(10)]
    assert sorted([row["multiply"] for row in results]) == [
        inspect.unwrap(multiply2)(i, i) for i in range(10)
    ]


def test_simple_agg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    count = gp.aggregate("count")

    results = list(count(numbers["val"], db=db).to_table().fetch())
    assert len(results) == 1 and results[0]["count"] == 10


def test_agg_group_by(db: gp.Database):
    rows = [(i, i % 2 == 0) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val", "is_even"])
    count = gp.aggregate("count")

    results = list(
        count(numbers["val"], group_by=numbers.group_by("is_even"), db=db).to_table().fetch()
    )
    assert len(results) == 2
    for row in results:
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["count"] == 5)


def test_agg_group_by_multi_columns(db: gp.Database):
    rows = [(i, i % 2 == 0, i % 3 == 0) for i in range(6)]  # 0, 1, 2, 3, 4, 5
    numbers = gp.values(rows, db=db, column_names=["val", "is_even", "is_multiple_of_3"])
    count = gp.aggregate("count")

    results = list(
        count(numbers["val"], group_by=numbers.group_by("is_even", "is_multiple_of_3"), db=db)
        .to_table()
        .fetch()
    )
    assert len(results) == 4  # 2 attributes * 2 possible values per attribute
    for row in results:
        assert (
            ("is_even" in row)
            and (row["is_even"] is not None)
            and ("is_multiple_of_3" in row)
            and (row["is_multiple_of_3"] is not None)
        )
        assert (
            (row["is_even"] and row["is_multiple_of_3"] and row["count"] == 1)  # 0
            or (row["is_even"] and not row["is_multiple_of_3"] and row["count"] == 2)  # 2, 4
            or (not row["is_even"] and row["is_multiple_of_3"] and row["count"] == 1)  # 3
            or (not row["is_even"] and not row["is_multiple_of_3"] and row["count"] == 2)  # 1, 5
        )


def test_create_agg(db: gp.Database):
    @gp.create_aggregate
    def my_sum(result: int, val: int) -> int:
        if result is None:
            return val
        return result + val

    rows = [(1,) for _ in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    results = list(my_sum(numbers["val"], as_name="result").to_table().fetch())
    assert len(results) == 1 and results[0]["result"] == 10


def test_create_agg_multi_args(db: gp.Database):
    @gp.create_aggregate
    def manhattan_distance(result: int, a: int, b: int) -> int:
        if result is None:
            return abs(a - b)
        return result + abs(a - b)

    rows = [(1, 2) for _ in range(10)]
    vectors = gp.values(rows, db=db, column_names=["a", "b"])
    results = list(
        manhattan_distance(vectors["a"], vectors["b"], as_name="result").to_table().fetch()
    )
    assert len(results) == 1 and results[0]["result"] == 10


def test_create_agg_optional_params(db: gp.Database):
    @gp.create_aggregate
    def my_sum(result: int, val: int) -> int:
        if result is None:
            return val
        return result + val

    @gp.create_aggregate(name="mysum")
    def my_sum(result: int, val: int) -> int:
        if result is None:
            return 5 + val
        return result + val

    rows = [(1,) for _ in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    results = list(my_sum(numbers["val"]).to_table().fetch())
    assert len(results) == 1 and results[0]["mysum"] == 15


def test_func_long_name(db: gp.Database):
    @gp.create_function
    def loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong() -> None:
        return

    with pytest.raises(Exception) as exc_info:
        loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong(db=db)
    # FIXME: Create more specific exception classes and remove this
    assert "Function name should be no longer than 63 bytes." == str(exc_info.value)


def test_array_func(db: gp.Database):
    @gp.create_array_function
    def my_sum(val_list: List[int]) -> int:
        return sum(val_list)

    rows = [(1,) for _ in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    results = list(my_sum(numbers["val"], as_name="result").to_table().fetch())
    assert len(results) == 1 and results[0]["result"] == 10


def test_array_func_group_by(db: gp.Database):
    @gp.create_array_function
    def my_sum(val_list: List[int]) -> int:
        return sum(val_list)

    rows = [(1, i % 2 == 0) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val", "is_even"])
    results = list(
        my_sum(numbers["val"], group_by=numbers.group_by("is_even"), as_name="result")
        .to_table()
        .fetch()
    )
    assert len(results) == 2
    assert list(list(results)[0].keys()) == ["result", "is_even"]
    for row in results:
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["result"] == 5)


def test_array_func_group_by_return_comp(db: gp.Database):
    class array_sum:
        _sum: int
        _count: int

    @gp.create_array_function
    def my_count_sum(val_list: List[int]) -> array_sum:
        return {"_sum": sum(val_list), "_count": len(val_list)}

    # fmt: off
    rows = [(1, "a",), (1, "a",), (1, "b",), (1, "a",), (1, "b",), (1, "b",)]
    # fmt: on
    numbers = gp.values(rows, db=db, column_names=["val", "lab"])
    ret = my_count_sum(numbers["val"], group_by=numbers.group_by("lab")).to_table().fetch()
    assert list(list(ret)[0].keys()) == ["_sum", "_count", "lab"]
    for row in list(ret):
        assert row["_sum"] == 3
        assert row["_count"] == 3


def test_array_func_replace(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])

    @gp.create_array_function
    def my_sum(val_list: List[int]) -> int:
        return sum(val_list)

    results = list(my_sum(numbers["val"], as_name="result").to_table().fetch())
    assert len(results) == 1 and results[0]["result"] == 10

    @gp.create_array_function(replace_if_exists=True)
    def my_sum(val_list: List[int]) -> int:
        return 5 + sum(val_list)

    results = list(my_sum(numbers["val"], as_name="result").to_table().fetch())
    assert len(results) == 1 and results[0]["result"] == 15


def test_func_return_comp_type(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    @gp.create_function
    def create_person(first: str, last: str) -> Person:
        return {"_first_name": first, "_last_name": last}

    for row in create_person("Amy", "An", db=db).to_table().fetch():
        assert row["_first_name"] == "Amy" and row["_last_name"] == "An"


def test_func_comp_type_column(db: gp.Database):
    class Pair:
        _num: int
        _next: int

    @gp.create_function
    def create_pair(num: int) -> Pair:
        return {"_num": num, "_next": num + 1}

    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    for row in create_pair(numbers["val"], db=db).to_table().fetch():
        assert row["_next"] == row["_num"] + 1


def test_func_comp_type_setof(db: gp.Database):
    class Pair:
        _num: int
        _next: int

    @gp.create_function
    def create_pair(num: int) -> List[Pair]:
        return [(num, num + 1) for _ in range(5)]

    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    ret = list(create_pair(numbers["val"], db=db).to_table().fetch())
    assert len(ret) == 50
    dict_record = {i: 0 for i in range(10)}
    for row in ret:
        dict_record[row["_num"]] += 1
        assert row["_next"] == row["_num"] + 1
    for key in dict_record:
        assert dict_record[key] == 5


def test_array_func_comp_type(db: gp.Database):
    class Stat:
        sum: int
        count: int

    @gp.create_array_function
    def my_stat(val_list: List[int]) -> Stat:
        return {"sum": sum(val_list), "count": len(val_list)}

    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    ret = list(my_stat(numbers["val"], db=db).to_table().fetch())
    for row in ret:
        assert row["sum"] == sum(list([i for i in range(10)])) and row["count"] == len(rows)


def test_func_apply_single_column(db: gp.Database):
    rows = [(i,) for i in range(-10, 0)]
    series = gp.values(rows, db=db, column_names=["id"])
    abs = gp.function("abs")
    result = series.apply(lambda t: abs(t["id"])).to_table().fetch()
    assert len(list(result)) == 10
    for row in result:
        assert row["abs"] >= 0


def test_func_apply_const_and_column(db: gp.Database):
    @gp.create_function
    def label(type_or_type: str, num: int) -> str:
        return type_or_type + str(num)

    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    result = numbers.apply(lambda t: label("label", t["val"])).to_table().fetch()
    assert len(list(result)) == 10
    for row in result:
        assert row["label"].startswith("label")


def test_func_apply_auto_column_mapping_select(db: gp.Database):
    @gp.create_function
    def my_sum(num1: int, num2: int) -> int:
        return num1 + num2

    # fmt: off
    rows = [(i, i,) for i in range(0, 10)]
    # fmt: on
    gp.values(rows, db=db, column_names=["n1", "n2"]).save_as("series", temp=True)
    results = db.get_table("series").apply(lambda t: my_sum(t["n1"], t["n2"])).to_table().fetch()
    assert sorted([row["my_sum"] for row in results]) == list(range(0, 20, 2))


def test_func_apply_auto_column_mapping_join(db: gp.Database):
    @gp.create_function
    def label(num: int, type_or_type: str) -> str:
        return ":".join([str(num), type_or_type])

    # fmt: off
    rows1 = [(1, "a1",), (2, "a2",), (3, "a3",)]
    rows2 = [(1, "b1",), (2, "b2",), (3, "b3",)]
    # fmt: on
    t1 = gp.values(rows1, db=db, column_names=["id1", "n1"])
    t2 = gp.values(rows2, db=db, column_names=["id2", "n2"])
    ret = t1.inner_join(
        t2,
        t1["id1"] == t2["id2"],
        targets=[t1["id1"], t2["n2"]],
    )
    result = ret.apply(lambda t: label(t["id1"], t["n2"])).to_table().fetch()
    for row in list(result):
        assert row["label"][1:3] == ":b"
        assert row["label"][0] == row["label"][3]


def test_func_comp_type_column_apply(db: gp.Database):
    class Pair:
        _num: int
        _next: int

    @gp.create_function
    def create_pair(num: int) -> Pair:
        return {"_num": num, "_next": num + 1}

    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    for row in numbers.apply(lambda tab: create_pair(tab["val"])).to_table().fetch():
        assert row["_next"] == row["_num"] + 1


def test_array_func_apply(db: gp.Database):
    @gp.create_array_function
    def my_sum(val_list: List[int]) -> int:
        return sum(val_list)

    rows = [(1,) for _ in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])

    results = list(numbers["val"].apply(my_sum).to_table().fetch())
    assert len(results) == 1 and results[0]["my_sum"] == 10


def test_array_func_group_by_comp_apply(db: gp.Database):
    class Stat:
        sum: int
        count: int

    @gp.create_array_function
    def my_stat(val_list: List[int]) -> Stat:
        return {"sum": sum(val_list), "count": len(val_list)}

    rows = [(1, i % 2 == 0) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val", "is_even"])
    results = list(
        numbers.group_by("is_even").apply(lambda tab: my_stat(tab["val"])).to_table().fetch()
    )
    assert list(list(results)[0].keys()) == ["sum", "count", "is_even"]
    for row in results:
        assert all(
            ["is_even" in row, row["is_even"] is not None, row["sum"] == 5, row["count"] == 5]
        )


def test_array_func_const_apply(db: gp.Database):
    @gp.create_array_function
    def my_sum(label: str, val_list: List[int], initial: int) -> str:
        return label + " : " + str(sum(val_list) + initial)

    rows = [(1,) for _ in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])

    results = list(numbers.apply(lambda tab: my_sum("sum", tab["val"], 5)).to_table().fetch())
    assert len(results) == 1 and results[0]["my_sum"] == "sum : 15"


def test_array_func_group_by_attribute(db: gp.Database):
    @gp.create_array_function
    def my_sum(label: str, val_list: List[int], initial: int) -> str:
        return label + " : " + str(sum(val_list) + initial)

    # fmt: off
    rows = [("a", i, 5,) for i in range(10)]
    # fmt: on
    numbers = gp.values(rows, db=db, column_names=["label", "val", "initial"])
    results = list(
        numbers.group_by("label", "initial")
        .apply(lambda tab: my_sum(tab["label"], tab["val"], tab["initial"]))
        .to_table()
        .fetch()
    )
    assert len(results) == 1 and results[0]["my_sum"] == "a : 50"
