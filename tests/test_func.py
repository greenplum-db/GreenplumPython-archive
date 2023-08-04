import sys
from typing import Callable, List

import pytest

import greenplumpython as gp
from greenplumpython.builtins.functions import count, generate_series
from greenplumpython.func import AggregateFunction, NormalFunction
from tests import db


@pytest.fixture
def series(db: gp.Database):
    rows = [(i, i) for i in range(10)]
    return db.create_dataframe(rows=rows, column_names=["a", "b"])


def test_plain_func(db: gp.Database):
    version = gp.function("version")
    for row in db.assign(version=lambda: version()):
        assert "Greenplum" in row["version"] or row["version"].startswith("PostgreSQL")


def test_schema_func(db: gp.Database):
    db._execute(
        f"""
        CREATE OR REPLACE FUNCTION test.test_schema_func(a int)
        RETURNS INTEGER AS
        $$
        return a
        $$
        LANGUAGE plpython3u
    """,
        has_results=False,
    )
    test_func = gp.function("test_schema_func", schema="test")

    # -- WITH ASSIGN FUNC
    for row in db.assign(result=lambda: test_func(1)):
        assert row["result"] == 1

    # -- WITH APPLY FUNC
    for row in db.apply(lambda: test_func(1), column_name="result"):
        assert row["result"] == 1


def test_set_returning_func(db: gp.Database):
    results = db.assign(id=lambda: generate_series(0, 9))
    assert sorted([row["id"] for row in results]) == list(range(10))


# TODO: Test other data types
def test_create_func(db: gp.Database):
    @gp.create_function
    def add(a: int, b: int) -> int:
        return a + b

    # -- WITH ASSIGN FUNC
    for row in db.assign(result=lambda: add(1, 2)):
        assert row["result"] == 1 + 2
        assert row["result"] == add.unwrap()(1, 2)

    # -- WITH APPLY FUNC
    for row in db.apply(lambda: add(1, 2), column_name="add"):
        assert row["add"] == 1 + 2
        assert row["add"] == add.unwrap()(1, 2)


def test_create_func_multiline(db: gp.Database):
    @gp.create_function
    def my_max(a: int, b: int) -> int:
        if a > b:
            return a
        else:
            return b

    # -- WITH ASSIGN FUNC
    for row in db.assign(result=lambda: my_max(1, 2)):
        assert row["result"] == max(1, 2)
        assert row["result"] == my_max.unwrap()(1, 2)

    # -- WITH APPLY FUNC
    for row in db.apply(lambda: my_max(1, 2), column_name="my_max"):
        assert row["my_max"] == max(1, 2)
        assert row["my_max"] == my_max.unwrap()(1, 2)


# fmt: off
def test_create_func_tab_indent(db: gp.Database):
	@gp.create_function
	def my_min(a: int, b: int) -> int:
		if a < b:
			return a
		else:
			return b

	for row in db.assign(result=lambda: my_min(1, 2)):
		assert row["result"] == min(1, 2)
		assert row["result"] == my_min.unwrap()(1, 2)
# fmt: on


def test_func_on_one_column(db: gp.Database):
    rows = [(i,) for i in range(-10, 0)]
    series = db.create_dataframe(rows=rows, column_names=["id"])
    abs = gp.function("abs")

    # -- WITH ASSIGN FUNC
    results = series.assign(abs=lambda nums: abs(nums["id"]))
    assert sorted([row["abs"] for row in results]) == list(range(1, 11))

    # -- WITH APPLY FUNC
    results2 = series.apply(lambda nums: abs(nums["id"]))
    assert sorted([row["abs"] for row in results2]) == list(range(1, 11))


def test_func_on_multi_columns(db: gp.Database, series: gp.DataFrame):
    @gp.create_function
    def multiply(a: int, b: int) -> int:
        return a * b

    # -- WITH ASSIGN FUNC
    results = series.assign(result=lambda t: multiply(t["a"], t["b"]))
    assert sorted([row["result"] for row in results]) == [i * i for i in range(10)]

    # -- WITH APPLY FUNC
    results = series.apply(lambda t: multiply(t["a"], t["b"]), column_name="result")
    assert sorted([row["result"] for row in results]) == [i * i for i in range(10)]


def test_func_on_more_than_one_dataframe(db: gp.Database):
    div = gp.function("div")
    rows = [(1,) for _ in range(10)]
    t1 = db.create_dataframe(rows=rows, column_names=["i"])
    t2 = db.create_dataframe(rows=rows, column_names=["i"])
    with pytest.raises(Exception) as exc_info:
        div(t1["i"], t2["i"])
    # FIXME: Create more specific exception classes and remove this
    assert "Cannot pass arguments from more than one dataframes" == str(exc_info.value)


def test_simple_agg(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])
    count = gp.aggregate_function("count")

    # -- WITH ASSIGN FUNC
    results = numbers.group_by().assign(count=lambda t: count(t["val"]))
    assert len(list(results)) == 1 and next(iter(results))["count"] == 10

    # -- WITH APPLY FUNC
    results = numbers.apply(lambda t: count(t["val"]))
    assert len(list(results)) == 1 and next(iter(results))["count"] == 10


def test_agg_group_by(db: gp.Database):
    rows = [(i, i % 2 == 0) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even"])
    count = gp.aggregate_function("count")

    # -- WITH ASSIGN FUNC
    results = numbers.group_by("is_even").assign(count=lambda t: count(t["val"]))
    for row in results:
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["count"] == 5)
    assert len(list(results)) == 2

    # -- WITH APPLY FUNC
    results = numbers.group_by("is_even").apply(lambda t: count(t["val"]))
    for row in results:
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["count"] == 5)
    assert len(list(results)) == 2


def test_agg_group_by_multi_columns(db: gp.Database):
    rows = [(i, i % 2 == 0, i % 3 == 0) for i in range(6)]  # 0, 1, 2, 3, 4, 5
    numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even", "is_multiple_of_3"])
    count = gp.aggregate_function("count")

    # -- WITH ASSIGN FUNC
    results = numbers.group_by("is_even", "is_multiple_of_3").assign(
        count=lambda t: count(t["val"])
    )
    assert len(list(results)) == 4  # 2 attributes * 2 possible values per attribute
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

    # -- WITH APPLY FUNC
    results = numbers.group_by("is_even", "is_multiple_of_3").apply(lambda t: count(t["val"]))
    assert len(list(results)) == 4  # 2 attributes * 2 possible values per attribute
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


@gp.create_aggregate
def my_sum(result: int, val: int) -> int:
    if result is None:
        return val
    return result + val


def test_create_agg(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    results = numbers.group_by().assign(result=lambda t: my_sum(t["val"]))
    assert len(list(results)) == 1 and next(iter(results))["result"] == 10

    # -- WITH APPLY FUNC
    results = numbers.group_by().apply(lambda t: my_sum(t["val"]), column_name="my_sum")
    assert len(list(results)) == 1 and next(iter(results))["my_sum"] == 10


def test_create_agg_multi_args(db: gp.Database):
    @gp.create_aggregate
    def manhattan_distance(result: int, a: int, b: int) -> int:
        if result is None:
            return abs(a - b)
        return result + abs(a - b)

    rows = [(1, 2) for _ in range(10)]
    vectors = db.create_dataframe(rows=rows, column_names=["a", "b"])

    # -- WITH ASSIGN FUNC
    results = vectors.group_by().assign(result=lambda t: manhattan_distance(t["a"], t["b"]))
    assert len(list(results)) == 1 and next(iter(results))["result"] == 10

    # -- WITH APPLY FUNC
    results = vectors.group_by().apply(
        lambda t: manhattan_distance(t["a"], t["b"]), column_name="result"
    )
    assert len(list(results)) == 1 and next(iter(results))["result"] == 10


def test_create_func_with_optional_param(db: gp.Database):
    @gp.create_function(language_handler="plcontainer")
    def func_opt_param() -> None:
        return

    assert isinstance(func_opt_param, NormalFunction)


def test_create_agg_with_optional_param(db: gp.Database):
    @gp.create_aggregate(language_handler="plcontainer")
    def agg_opt_param() -> None:
        return

    assert isinstance(agg_opt_param, AggregateFunction)


@gp.create_column_function
def my_sum_array(val_list: List[int]) -> int:
    return sum(val_list)


def test_array_func(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    results = numbers.group_by().assign(result=lambda t: my_sum_array(t["val"]))
    assert len(list(results)) == 1 and next(iter(results))["result"] == 10

    # -- WITH APPLY FUNC
    results = numbers.group_by().apply(lambda t: my_sum_array(t["val"]), column_name="result")
    assert len(list(results)) == 1 and next(iter(results))["result"] == 10


def test_array_func_group_by(db: gp.Database):
    rows = [(1, i % 2 == 0) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even"])

    # -- WITH ASSIGN FUNC
    results = numbers.group_by("is_even").assign(result=lambda t: my_sum_array(t["val"]))
    assert len(list(results)) == 2
    assert all(e in next(iter(results)).keys() for e in ["result", "is_even"])
    for row in results:
        print(row["is_even"])
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["result"] == 5)

    # -- WITH APPLY FUNC
    results = numbers.group_by("is_even").apply(
        lambda t: my_sum_array(t["val"]), column_name="my_sum"
    )
    assert len(list(results)) == 2
    assert all(e in next(iter(results)).keys() for e in ["my_sum", "is_even"])
    for row in results:
        print(row["is_even"])
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["my_sum"] == 5)


from types import SimpleNamespace


def test_array_func_group_by_return_composite(db: gp.Database):
    class array_sum(SimpleNamespace):
        _sum: int
        _count: int

    @gp.create_column_function
    def my_count_sum(val_list: List[int]) -> array_sum:
        return SimpleNamespace(_sum=sum(val_list), _count=len(val_list))

    rows = [(1, "a"), (1, "a"), (1, "b"), (1, "a"), (1, "b"), (1, "b")]
    numbers = db.create_dataframe(rows=rows, column_names=["val", "lab"])

    # -- WITH ASSIGN FUNC
    ret = (
        numbers.group_by("lab")
        .assign(result=lambda t: my_count_sum(t["val"]))
        .assign(_sum=lambda t: t["result"]["_sum"], _count=lambda t: t["result"]["_count"])
    )
    assert all(e in next(iter(ret)).keys() for e in ["_sum", "_count", "lab"])
    for row in ret:
        assert row["_sum"] == 3
        assert row["_count"] == 3

    # -- WITH APPLY FUNC
    ret_apply = numbers.group_by("lab").apply(lambda t: my_count_sum(t["val"]), expand=True)
    print(next(iter(ret_apply)).keys())
    assert all(e in next(iter(ret_apply)).keys() for e in ["_sum", "_count", "lab"])
    for row in ret_apply:
        assert row["_sum"] == 3
        assert row["_count"] == 3

    class Person(SimpleNamespace):
        _first_name: str
        _last_name: str

    @gp.create_function
    def create_person(first: str, last: str) -> Person:
        return SimpleNamespace(_first_name=first, _last_name=last)

    # -- WITH ASSIGN FUNC
    for row in db.assign(result=lambda: create_person("Amy", "An")).assign(
        first_name=lambda t: t["result"]["_first_name"],
        last_name=lambda t: t["result"]["_last_name"],
    ):
        assert row["first_name"] == "Amy" and row["last_name"] == "An"

    # -- WITH APPLY FUNC
    for row in db.apply(lambda: create_person("Amy", "An"), expand=True):
        assert row["_first_name"] == "Amy" and row["_last_name"] == "An"


class Pair(SimpleNamespace):
    _num: int
    _next: int


@gp.create_function
def create_pair(num: int) -> Pair:
    return SimpleNamespace(_num=num, _next=num + 1)


def test_func_composite_type_column(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    for row in numbers.assign(result=lambda t: create_pair(t["val"])).assign(
        _next=lambda t: t["result"]["_next"], _num=lambda t: t["result"]["_num"]
    ):
        assert row["_next"] == row["_num"] + 1

    # -- WITH APPLY FUNC
    for row in numbers.apply(lambda t: create_pair(t["val"]), expand=True):
        assert row["_next"] == row["_num"] + 1


def test_func_composite_type_setof(db: gp.Database):
    class Pair(SimpleNamespace):
        _num: int
        _next: int

    @gp.create_function
    def create_pair_tuple(num: int) -> List[Pair]:
        return [SimpleNamespace(_num=num, _next=num + 1) for _ in range(5)]

    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    ret = numbers.assign(result=lambda t: create_pair_tuple(t["val"])).assign(
        _next=lambda t: t["result"]["_next"], _num=lambda t: t["result"]["_num"]
    )
    assert len(list(ret)) == 50
    dict_record = {i: 0 for i in range(10)}
    for row in ret:
        dict_record[row["_num"]] += 1
        assert row["_next"] == row["_num"] + 1
    for key in dict_record:
        assert dict_record[key] == 5

    # -- WITH APPLY FUNC
    ret_apply = numbers.apply(lambda t: create_pair_tuple(t["val"]), expand=True)
    assert len(list(ret_apply)) == 50
    dict_record = {i: 0 for i in range(10)}
    for row in ret_apply:
        dict_record[row["_num"]] += 1
        assert row["_next"] == row["_num"] + 1
    for key in dict_record:
        assert dict_record[key] == 5


class Stat(SimpleNamespace):
    sum: int
    count: int


@gp.create_column_function
def my_stat(val_list: List[int]) -> Stat:
    return SimpleNamespace(sum=sum(val_list), count=len(val_list))


def test_array_func_composite_type(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    ret = (
        numbers.group_by()
        .assign(result=lambda t: my_stat(t["val"]))
        .assign(sum=lambda t: t["result"]["sum"], count=lambda t: t["result"]["count"])
    )
    for row in ret:
        assert row["sum"] == sum(list([i for i in range(10)])) and row["count"] == len(rows)

    # -- WITH APPLY FUNC
    ret_apply = numbers.group_by().apply(lambda t: my_stat(t["val"]), expand=True)
    for row in ret_apply:
        assert row["sum"] == sum(list([i for i in range(10)])) and row["count"] == len(rows)


def test_func_apply_single_column(db: gp.Database):
    rows = [(i,) for i in range(-10, 0)]
    series = db.create_dataframe(rows=rows, column_names=["id"])
    abs = gp.function("abs")

    # -- WITH ASSIGN FUNC
    result = series.assign(abs=lambda t: abs(t["id"]))
    assert len(list(result)) == 10
    for row in result:
        assert row["abs"] >= 0

    # -- WITH APPLY FUNC
    result = series.apply(lambda t: abs(t["id"]))
    assert len(list(result)) == 10
    for row in result:
        assert row["abs"] >= 0


@gp.create_function
def label(type_or_type: str, num: int) -> str:
    return type_or_type + str(num)


def test_func_apply_const_and_column(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    result = numbers.assign(label=lambda t: label("label", t["val"]))
    assert len(list(result)) == 10
    for row in result:
        assert row["label"].startswith("label")

    # -- WITH APPLY FUNC
    result = numbers.apply(lambda t: label("label", t["val"]), column_name="label")
    assert len(list(result)) == 10
    for row in result:
        assert row["label"].startswith("label")


def test_func_apply_join(db: gp.Database):
    # fmt: off
    rows1 = [(1, "a1",), (2, "a2",), (3, "a3",)]
    rows2 = [(1, "b1",), (2, "b2",), (3, "b3",)]
    # fmt: on
    t1 = db.create_dataframe(rows=rows1, column_names=["id1", "n1"])
    t2 = db.create_dataframe(rows=rows2, column_names=["id2", "n2"])
    ret = t1.join(
        t2, cond=lambda t1, t2: t1["id1"] == t2["id2"], self_columns={"id1"}, other_columns={"n2"}
    )

    # -- WITH ASSIGN FUNC
    result = ret.assign(label=lambda t: label(t["n2"], t["id1"]))
    for row in result:
        assert row["label"][1] == row["label"][2]

    # -- WITH APPLY FUNC
    result = ret.apply(lambda t: label(t["n2"], t["id1"]), column_name="label")
    for row in result:
        assert row["label"][1] == row["label"][2]


def test_func_composite_type_column_apply(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    for row in numbers.assign(result=lambda tab: create_pair(tab["val"])).assign(
        _next=lambda t: t["result"]["_next"], _num=lambda t: t["result"]["_num"]
    ):
        assert row["_next"] == row["_num"] + 1

    # -- WITH APPLY FUNC
    for row in numbers.apply(lambda tab: create_pair(tab["val"]), expand=True):
        assert row["_next"] == row["_num"] + 1


def test_array_func_apply(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    results = numbers.group_by().assign(my_sum=lambda t: my_sum_array(t["val"]))
    assert len(list(results)) == 1 and next(iter(results))["my_sum"] == 10

    # -- WITH APPLY FUNC
    results = numbers.group_by().apply(lambda t: my_sum_array(t["val"]), column_name="my_sum_array")
    assert len(list(results)) == 1 and next(iter(results))["my_sum_array"] == 10


def test_array_func_group_by_composite_apply(db: gp.Database):
    rows = [(1, i % 2 == 0) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even"])

    # -- WITH ASSIGN FUNC
    results = (
        numbers.group_by("is_even")
        .assign(result=lambda tab: my_stat(tab["val"]))
        .assign(sum=lambda t: t["result"]["sum"], count=lambda t: t["result"]["sum"])
    )
    assert all(e in next(iter(results)).keys() for e in ["sum", "count", "is_even"])
    for row in results:
        assert all(
            ["is_even" in row, row["is_even"] is not None, row["sum"] == 5, row["count"] == 5]
        )

    # -- WITH APPLY FUNC
    results = numbers.group_by("is_even").apply(lambda tab: my_stat(tab["val"]), expand=True)
    assert all(e in next(iter(results)).keys() for e in ["sum", "count", "is_even"])
    for row in results:
        assert all(
            ["is_even" in row, row["is_even"] is not None, row["sum"] == 5, row["count"] == 5]
        )


@gp.create_column_function
def my_sum_const(label: str, val_list: List[int], initial: int) -> str:
    return label + " : " + str(sum(val_list) + initial)


def test_array_func_const_apply(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    # -- WITH ASSIGN FUNC
    results = numbers.group_by().assign(my_sum=lambda tab: my_sum_const("sum", tab["val"], 5))
    assert len(list(results)) == 1 and next(iter(results))["my_sum"] == "sum : 15"

    # -- WITH APPLY FUNC
    results = numbers.group_by().apply(
        lambda tab: my_sum_const("sum", tab["val"], 5), column_name="my_sum_const"
    )
    assert len(list(results)) == 1 and next(iter(results))["my_sum_const"] == "sum : 15"


def test_array_func_group_by_attribute(db: gp.Database):
    # fmt: off
    rows = [("a", i, 5,) for i in range(10)]
    # fmt: on
    numbers = db.create_dataframe(rows=rows, column_names=["label", "val", "initial"])

    # -- WITH ASSIGN FUNC
    results = numbers.group_by("label", "initial").assign(
        my_sum=lambda tab: my_sum_const(tab["label"], tab["val"], tab["initial"])
    )
    assert len(list(results)) == 1 and next(iter(results))["my_sum"] == "a : 50"

    # -- WITH APPLY FUNC
    results = numbers.group_by("label", "initial").apply(
        lambda tab: my_sum_const(tab["label"], tab["val"], tab["initial"]),
        column_name="my_sum_const",
    )
    assert len(list(results)) == 1 and next(iter(results))["my_sum_const"] == "a : 50"


def test_func_return_list_composite(db: gp.Database):
    class ShoppingCart(SimpleNamespace):
        customer: str
        items: List[str]

    @gp.create_function
    def add_to_cart(customer: str, items: List[str]) -> ShoppingCart:
        return SimpleNamespace(customer=customer, items=items)

    # -- WITH ASSIGN FUNC
    results = db.assign(result=lambda: add_to_cart("alice", ["apple"])).assign(
        customer=lambda t: t["result"]["customer"], items=lambda t: t["result"]["items"]
    )
    for row in results:
        assert row["customer"] == "alice" and row["items"] == ["apple"]

    # -- WITH APPLY FUNC
    results = db.apply(lambda: add_to_cart("alice", ["apple"]), expand=True)
    for row in results:
        assert row["customer"] == "alice" and row["items"] == ["apple"]


def test_create_func_same_name(db: gp.Database):
    @gp.create_function
    def dup_name(a: int, b: int) -> int:
        return a + b

    _, func_name = dup_name._qualified_name

    @gp.create_function
    def dup_name(a: int, b: int) -> int:
        return a + 1

    _, new_func_name = dup_name._qualified_name

    assert func_name != new_func_name


def test_agg_returning_dataframe(db: gp.Database):
    @gp.create_aggregate
    def pass_agg(result: List[int], val: int) -> List[int]:
        if result is None:
            return [val]
        return result + [val]

    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])
    # -- WITH ASSIGN FUNC
    with pytest.raises(Exception):  # state transition functions may not return table
        numbers.group_by().assign(result=lambda t: pass_agg(t["val"]))


def test_agg_distinct(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])

    count = gp.aggregate_function("count")
    result = numbers.group_by().assign(
        count=lambda t: count(t["val"]), count_distinct=lambda t: count.distinct(t["val"])
    )
    for row in result:
        assert row["count"] == len(rows) and row["count_distinct"] == len(set(rows)) == 1


def test_agg_composite_type(db: gp.Database):
    class sum_count_type(SimpleNamespace):
        sum: int
        count: int

    @gp.create_aggregate
    def sum_count(result: sum_count_type, val: int) -> sum_count_type:
        if result is None:
            return SimpleNamespace(sum=val, count=1)
        return SimpleNamespace(sum=result["sum"] + val, count=result["count"] + 1)

    rows = [(i,) for i in range(10)]
    numbers = db.create_dataframe(rows=rows, column_names=["val"])
    result = numbers.group_by().apply(lambda t: sum_count(t["val"]), expand=True)
    for row in result:
        assert row["count"] == len(rows) and row["sum"] == 45


def test_func_plpy(db: gp.Database):
    @gp.create_function
    def test_plpy() -> bool:
        created_with_dill = not any([name.startswith("__func_") for name in globals().keys()])
        # Function with name starting with "__func_" will only be created by
        # exec() when using source serialization, i.e. when dill is not
        # available on server.
        #
        # If dill is used for deserialization, plpy will not be available.
        return created_with_dill or "plpy" in globals().keys()

    df = db.apply(lambda: test_plpy(), column_name="result")
    for row in df:
        assert row["result"]


import math


def test_func_with_outside_imports(db: gp.Database):
    # NOTE: imports in function's closure rather than in globals() is NOT
    # supported.
    @gp.create_function
    def my_math(x: int) -> float:
        return math.sqrt(x**2)

    df = db.apply(lambda: my_math(42), column_name="x")
    assert len(list(df)) == 1
    for row in df:
        assert abs(row["x"] - 42) < 1e-5


def test_func_with_outside_func(db: gp.Database):
    def inner(x: int) -> int:
        return x * x

    @gp.create_function
    def proxy(x: int) -> float:
        created_with_dill = not any([name.startswith("__func_") for name in globals().keys()])
        # Fallback when the pickle lib "dill" is not available on server.
        if created_with_dill:
            return inner(x)
        return x * x

    df = db.apply(lambda: proxy(5), column_name="x")
    assert len(list(df)) == 1
    for row in df:
        assert row["x"] == 25


from dataclasses import dataclass


def test_func_with_outside_class(db: gp.Database):
    # It is recommended to use dataclass to represent composite types.
    @dataclass
    class Student(SimpleNamespace):
        name: str
        age: int

    @gp.create_function
    def student(name: str, age: int) -> Student:
        created_with_dill = not any([name.startswith("__func_") for name in globals().keys()])
        # Fallback when the pickle lib "dill" is not available on server.
        if created_with_dill:
            return Student(name, age)
        return SimpleNamespace(name=name, age=age)

    df = db.apply(lambda: student("alice", 19), expand=True)
    assert len(list(df)) == 1
    for row in df:
        assert row["name"] == "alice" and row["age"] == 19


def test_func_one_liner(db: gp.Database):
    # fmt: off
    @gp.create_function
    def add_one(x: int) -> int: return x + 1
    # fmt: on

    df = db.apply(lambda: add_one(1), column_name="x")
    assert len(list(df)) == 1
    for row in df:
        assert row["x"] == 2

    # TODO: Lambda expressions are not supported.
    add_one: Callable[[int], int] = lambda x: x + 1
    with pytest.raises(AssertionError) as exc_info:
        db.apply(lambda: gp.create_function(add_one)(1), column_name="x")
    assert "is not a function" in str(exc_info.value)


def test_func_non_default_schema(db: gp.Database):
    rows = [(i,) for i in range(-10, 0)]
    db.create_dataframe(rows=rows, column_names=["id"]).save_as(
        table_name="test_func_schema", column_names=["id"], schema="test"
    )
    series = db.create_dataframe(table_name="test_func_schema", schema="test")
    abs = gp.function("abs")

    # -- WITH ASSIGN FUNC
    results = series.assign(abs=lambda nums: abs(nums["id"]))
    assert sorted([row["abs"] for row in results]) == list(range(1, 11))

    # -- WITH APPLY FUNC
    results2 = series.apply(lambda nums: abs(nums["id"]))
    assert sorted([row["abs"] for row in results2]) == list(range(1, 11))


@gp.create_function
def add_one(x: int) -> int:
    return x + 1


@gp.create_function
def add_two(x: int) -> int:
    return x + 2


def test_func_nested_create(db: gp.Database):
    result = db.apply(lambda: add_two(add_one(1)), column_name="val")
    for row in result:
        assert row["val"] == 1 + 1 + 2


def test_count_none(db: gp.Database):
    for row in db.create_dataframe(columns={"none": [1, None]}).apply(
        lambda _: count(), column_name="count"
    ):
        assert row["count"] == 2


def test_func_in_binary_expr(db: gp.Database):
    result = db.assign(val=lambda: add_two(1) + add_one(1))
    for row in result:
        assert row["val"] == (1 + 2) + (1 + 1)


def test_func_in_where(db: gp.Database):
    df = db.create_dataframe(columns={"a": [1]})
    result = df.where(lambda t: add_two(t["a"]) < 5)
    assert len(list(result)) == 1
