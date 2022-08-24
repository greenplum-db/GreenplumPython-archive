from functools import partial

import greenplumpython as gp
from tests import db


def test_group_agg(db: gp.Database):
    rows = [(i, i % 2 == 0) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val", "is_even"])
    count = gp.aggregate("count")

    results = list(
        numbers.group_by("is_even").apply(lambda row: count(row["*"])).to_table().fetch()
    )
    assert len(results) == 2
    for row in results:
        assert ("is_even" in row) and (row["is_even"] is not None) and (row["count"] == 5)


def test_group_agg_multi_columns(db: gp.Database):
    rows = [(i, i, i % 2 == 0) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val", "val_cp", "is_even"])

    @gp.create_aggregate
    def my_sum(result: int, val: int, val_cp: int) -> int:
        if result is None:
            return val + val_cp
        return result + val + val_cp

    results = list(
        numbers.group_by("is_even")
        .apply(lambda row: my_sum(row["val"], row["val_cp"]))
        .to_table()
        .fetch()
    )
    assert len(results) == 2
    for row in results:
        assert ("is_even" in row) and (row["is_even"] is not None)
        assert (row["is_even"] and row["my_sum"] == 2 * sum(list(range(0, 10, 2)))) or (
            (not row["is_even"]) and row["my_sum"] == 2 * sum(list(range(1, 10, 2)))
        )


def test_group_by_multi_columns(db: gp.Database):
    rows = [(i, i % 2 == 0, i % 3 == 0) for i in range(6)]  # 0, 1, 2, 3, 4, 5
    numbers = gp.values(rows, db=db, column_names=["val", "is_even", "is_multiple_of_3"])
    count = gp.aggregate("count")

    results = list(
        numbers.group_by("is_even", "is_multiple_of_3")
        .apply(lambda row: count(row["val"]))
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


def test_group_union(db: gp.Database):
    rows = [(i, i % 2 == 0, i % 3 == 0) for i in range(6)]  # 0, 1, 2, 3, 4, 5
    numbers = gp.values(rows, db=db, column_names=["val", "is_even", "is_multiple_of_3"])
    count = gp.aggregate("count")

    results = list(
        (numbers.group_by("is_even") | numbers.group_by("is_multiple_of_3"))
        .apply(lambda row: count(row["val"]))
        .to_table()
        .fetch()
    )
    assert len(results) == 4  # 2 attributes * 2 possible values per attribute
    for row in results:
        assert ("is_even" in row) and ("is_multiple_of_3" in row)
        assert (
            (row["is_even"] and row["is_multiple_of_3"] is None and row["count"] == 3)
            or (not row["is_even"] and row["is_multiple_of_3"] is None and row["count"] == 3)
            or (row["is_even"] is None and row["is_multiple_of_3"] and row["count"] == 2)
            or (row["is_even"] is None and not row["is_multiple_of_3"] and row["count"] == 4)
        )
